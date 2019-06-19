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


import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.file.FileMgr;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 */
class BufferPoolMgr {
	private Buffer[] bufferPool;
	private Map<BlockId, Buffer> blockMap;
	private AtomicInteger numAvailable;
	// Optimization: Lock striping
	private Object[] anchors = new Object[1009];
	private int numBuffs;
	public int cacheHit = 1, cacheMiss = 1;
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
		this.numBuffs = numBuffs;
		bufferPool = new Buffer[numBuffs];
		blockMap = new ConcurrentHashMap<BlockId, Buffer>();
		numAvailable = new AtomicInteger(numBuffs);		
		
		for (int i = 0; i < numBuffs; i++)
			bufferPool[i] = new Buffer();

		for (int i = 0; i < anchors.length; ++i) {
			anchors[i] = new Object();
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
		synchronized(bufferPool)
		{
			for (Buffer buff : bufferPool) {
					buff.flush();
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
		synchronized(bufferPool)
		{
			for (Buffer buff : bufferPool) {
				if (buff.isModifiedBy(txNum)) {
					buff.flush();
				}
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
		//System.out.printf("Cache miss: %d , Cache hit: %d , Hit rate : %f\r",cacheMiss,cacheHit,(float)cacheHit/((float)cacheMiss+(float)cacheHit));
		// Need to sync on BFP
		synchronized (bufferPool) {
			// Find existing buffer
			Buffer buff = findExistingBuffer(blk);

			// If there is no such buffer
			if (buff == null) {
				
				// Try to find replaceable buffer
				long nowTime = System.nanoTime();
				long minTime = nowTime;

				for(int i=0;i<numBuffs;i++)
				{
					if(!bufferPool[i].isPinned())
					{
						if(bufferPool[i].blk==null)
						{
							buff = bufferPool[i];
							break;
						}
						if(nowTime-bufferPool[i].blk.last_reference_time>BlockId.CRT && bufferPool[i].blk.order()<minTime)
						{
							buff = bufferPool[i];
							minTime = bufferPool[i].blk.order();
						}
					}
				}
				if(buff==null)
					return null;
				
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
				
				// Check if this blockId is new
				if(blk.hist[BlockId.LRU_K-1]==0)
					blk.updateHistM();
				
				// CacheMiss
				//cacheMiss++;
				
				return buff;
			}
			// If it exists
			else {
				if (buff.block().equals(blk)) {
					if (!buff.isPinned()) {
						numAvailable.decrementAndGet();
					}
					buff.pin();
					buff.blk.updateHist();
					// CacheHit
					//cacheHit++;
					return buff;
				}
				return pin(blk);
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
		synchronized (bufferPool) {
			Buffer buff=null;
			// Try to find replaceable buffer
			long nowTime = System.nanoTime();
			long minTime = nowTime;

			for(int i=0;i<numBuffs;i++)
			{
				if(!bufferPool[i].isPinned())
				{
					if(bufferPool[i].blk==null)
					{
						buff = bufferPool[i];
						break;
					}
					if(nowTime-bufferPool[i].blk.last_reference_time>BlockId.CRT && bufferPool[i].blk.order()<minTime)
					{
						buff = bufferPool[i];
						minTime = bufferPool[i].blk.order();
					}
				}
			}
			if(buff==null)
				return null;
				
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
			buff.blk.updateHistM();
			return buff;
			}
		}


	/**
	 * Unpins the specified buffers.
	 * 
	 * @param buffs
	 *            the buffers to be unpinned
	 */
	void unpin(Buffer... buffs) {
		synchronized(bufferPool)
		{
			for (Buffer buff : buffs) {
				buff.unpin();
				if (!buff.isPinned())
				{
					numAvailable.incrementAndGet();
				}
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
