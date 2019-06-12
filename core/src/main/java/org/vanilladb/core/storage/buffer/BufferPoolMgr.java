/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.core.storage.buffer;

import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.file.FileMgr;
import org.vanilladb.core.storage.file.Page;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 */
class BufferPoolMgr {
	private Buffer[] bufferPool;
	private Map<BlockId, Buffer> blockMap;
	private AtomicInteger numAvailable;
	private PriorityQueue<Buffer> unpinnedHeap;
	// Optimization: Lock striping
	private Object[] anchors = new Object[1009];
	
	/**
	 * Creates a buffer manager having the specified number of buffer slots.
	 * This constructor depends on both the {@link FileMgr} and
	 * {@link org.vanilladb.core.storage.log.LogMgr LogMgr} objects that it gets
	 * from the class {@link VanillaDb}. Those objects are created during system
	 * initialization. Thus this constructor cannot be called until
	 * {@link VanillaDb#initFileAndLogMgr(String)} or is called first.
	 * 
	 * @param numBuffs
	 *            the number of buffer slots to allocate
	 */
	BufferPoolMgr(int numBuffs) {
		bufferPool = new Buffer[numBuffs];
		blockMap = new ConcurrentHashMap<BlockId, Buffer>();
		numAvailable = new AtomicInteger(numBuffs);		
		// smaller time_stamp should be on top of the heap. 
		Comparator<Buffer> heapComparator = (b1, b2) -> {
			// check if blk is null (will happen at the beginning)
			if(b1.blk == null)
				return -1;
			if(b2.blk == null)
				return 1;
			return (int)(b1.blk.order()-b2.blk.order());
	    };

		unpinnedHeap = new PriorityQueue<>(numBuffs,heapComparator);
		
		for (int i = 0; i < numBuffs; i++)
			bufferPool[i] = new Buffer();

		for (int i = 0; i < anchors.length; ++i) {
			anchors[i] = new Object();
		}
		
		// add all Blocks(Pages) into queue first.
		for(int i=0;i<numBuffs;i++)
		{
			unpinnedHeap.add(bufferPool[i]);
		}
				
	}

	// Optimization: Lock striping
	private Object prepareAnchor(Object o) {
		int code = o.hashCode() % anchors.length;
		if (code < 0)
			code += anchors.length;
		return anchors[code];
	}

	/**
	 * Flushes all dirty buffers.
	 */
	void flushAll() {
		for (Buffer buff : bufferPool) {
			try {
				buff.getExternalLock().lock();
				buff.flush();
			} finally {
				buff.getExternalLock().unlock();
			}
		}
	}

	/**
	 * Flushes the dirty buffers modified by the specified transaction.
	 * 
	 * @param txNum
	 *            the transaction's id number
	 */
	void flushAll(long txNum) {
		for (Buffer buff : bufferPool) {
			try {
				buff.getExternalLock().lock();
				if (buff.isModifiedBy(txNum)) {
					buff.flush();
				}
			} finally {
				buff.getExternalLock().unlock();
			}
		}
	}

	/**
	 * Pins a buffer to the specified block. If there is already a buffer
	 * assigned to that block then that buffer is used; otherwise, an unpinned
	 * buffer from the pool is chosen. Returns a null value if there are no
	 * available buffers.
	 * 
	 * @param blk
	 *            a block ID
	 * @return the pinned buffer
	 */
	Buffer pin(BlockId blk) {
		// Only the txs acquiring the same block will be blocked
		synchronized (prepareAnchor(blk)) {
			// Find existing buffer
			Buffer buff = findExistingBuffer(blk);

			// If there is no such buffer
			if (buff == null) {
				synchronized(unpinnedHeap)
				{
					if(unpinnedHeap.isEmpty())
						return null;
					buff = unpinnedHeap.poll();
				}
				// Get the lock of buffer if it is free
				if (buff.getExternalLock().tryLock()) {
					try {
						// Swap
						BlockId oldBlk = buff.block();
						if (oldBlk != null)
							blockMap.remove(oldBlk);
						buff.assignToBlock(blk);
						blockMap.put(blk, buff);
						if (!buff.isPinned())
							numAvailable.decrementAndGet();
						
						// Pin this buffer
						buff.pin();
						return buff;
						
					} finally {
						// Release the lock of buffer
						buff.getExternalLock().unlock();
					}
				}
				return null;
			}
			// If it exists
			else {
				// Get the lock of buffer
				buff.getExternalLock().lock();
				
				try {
					// Check its block id before pinning since it might be swapped
					if (buff.block().equals(blk)) {
						if (!buff.isPinned()) {
							numAvailable.decrementAndGet();
							synchronized(unpinnedHeap)
							{
								unpinnedHeap.remove(buff);
							}
						}
						buff.pin();
						buff.blk.updateHist();
						return buff;
					}
					return pin(blk);
					
				} finally {
					// Release the lock of buffer
					buff.getExternalLock().unlock();
				}
			}
		}
	}

	/**
	 * Allocates a new block in the specified file, and pins a buffer to it.
	 * Returns null (without allocating the block) if there are no available
	 * buffers.
	 * 
	 * @param fileName
	 *            the name of the file
	 * @param fmtr
	 *            a pageformatter object, used to format the new block
	 * @return the pinned buffer
	 */
	Buffer pinNew(String fileName, PageFormatter fmtr) {
		// Only the txs acquiring to append the block on the same file will be blocked
		synchronized (prepareAnchor(fileName)) {
			Buffer buff;
			// Choose Unpinned Buffer
			synchronized(unpinnedHeap)
			{
				if(unpinnedHeap.isEmpty())
					return null;
				buff = unpinnedHeap.poll();
			}
				// Get the lock of buffer if it is free
				if (buff.getExternalLock().tryLock()) {
					try {
						// Swap
						BlockId oldBlk = buff.block();
						if (oldBlk != null)
							blockMap.remove(oldBlk);
						buff.assignToNew(fileName, fmtr);
						blockMap.put(buff.block(), buff);
						if (!buff.isPinned())
							numAvailable.decrementAndGet();
						
						// Pin this buffer
						buff.pin();
						return buff;
					} finally {
						// Release the lock of buffer
						buff.getExternalLock().unlock();
					}
				}
			}
			return null;
		}


	/**
	 * Unpins the specified buffers.
	 * 
	 * @param buffs
	 *            the buffers to be unpinned
	 */
	void unpin(Buffer... buffs) {
		for (Buffer buff : buffs) {
			try {
				// Get the lock of buffer
				buff.getExternalLock().lock();
				buff.unpin();
				if (!buff.isPinned())
				{
					numAvailable.incrementAndGet();
					// If this buffer is not pinned, we put it into the heap.
					synchronized(unpinnedHeap)
					{
						unpinnedHeap.add(buff);
					}
				}
			} finally {
				// Release the lock of buffer
				buff.getExternalLock().unlock();
			}
		}
	}

	/**
	 * Returns the number of available (i.e. unpinned) buffers.
	 * 
	 * @return the number of available buffers
	 */
	int available() {
		return numAvailable.get();
	}

	private Buffer findExistingBuffer(BlockId blk) {
		Buffer buff = blockMap.get(blk);
		if (buff != null && buff.block().equals(blk))
			return buff;
		return null;
	}
}
