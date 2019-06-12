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
package org.vanilladb.core.storage.file;

import org.vanilladb.core.util.CoreProperties;

/**
 * A reference to a disk block. A BlockId object consists of a fileName and a
 * block number. It does not hold the contents of the block; instead, that is
 * the job of a {@link Page} object.
 */
public class BlockId implements Comparable<BlockId> {
	private String fileName;
	private long blkNum;
	// Optimization: Materialize the toString and hash value
	private String myString;
	private int myHashCode;

	// LRU-K
	public static final int LRU_K;
	static {
		LRU_K = CoreProperties.getLoader().getPropertyAsInteger(Page.class.getName() + ".LRU_K", 2);
	}
	
	// Correlated Reference Time
	public static final int CRT;
	static {
		CRT = CoreProperties.getLoader().getPropertyAsInteger(Page.class.getName() + ".CRT", 100000);
	}
		
	// Retain Info Time
	public static final int RIT;
	static {
		RIT = CoreProperties.getLoader().getPropertyAsInteger(Page.class.getName() + ".RIT", 200000);
	}		

	
	// LAST(p) in LRU_K algorithm.
	public long last_reference_time;
	
	// HIST(p,...) in LRU_K algorithm.
	public long[] hist = new long[LRU_K];
	
	// Reset HIST.
	public void initHist()
	{
		hist[0] = System.nanoTime();
		for(int i=1;i<LRU_K;i++)
			hist[i] = 0;
	}
	
	// Return the time of LRU_K.
	public long order()
	{
		return hist[LRU_K-1];
	}	
	
	// Reference this BlockId (When cache misses)
	public void updateHistM() 
	{
		long nowTime = System.nanoTime();
		for(int i=1;i<LRU_K;i++)
			hist[i] = hist[i-1];
		last_reference_time = nowTime;
		hist[0] = nowTime;
	}
	
	
	// Reference this BlockId (When cache hits)
	public void updateHist() 
	{
		long nowTime = System.nanoTime();
		// check if not in CRT
		if(nowTime/1000000-last_reference_time/1000000>CRT)
		{
			long correl_period_of_refd_page = last_reference_time - hist[0];
			for(int i=1;i<LRU_K;i++)
				hist[i] = hist[i-1] + correl_period_of_refd_page;
			hist[0] = nowTime;
		}
		last_reference_time = nowTime;
	}
	
	/**
	 * Constructs a block ID for the specified fileName and block number.
	 * 
	 * @param fileName
	 *            the name of the file
	 * @param blkNum
	 *            the block number
	 */
	public BlockId(String fileName, long blkNum) {
		this.fileName = fileName;
		this.blkNum = blkNum;
		// Optimization: Materialize the hash code and the output of toString
		myString = "[file " + fileName + ", block " + blkNum + "]";
		myHashCode = myString.hashCode();
		initHist();
		last_reference_time = System.nanoTime();
	}

	/**
	 * Returns the name of the file where the block lives.
	 * 
	 * @return the fileName
	 */
	public String fileName() {
		return fileName;
	}

	/**
	 * Returns the location of the block within the file.
	 * 
	 * @return the block number
	 */
	public long number() {
		return blkNum;
	}
	
	@Override
	public int compareTo(BlockId blk) {
		int nameResult = fileName.compareTo(blk.fileName);
		if (nameResult != 0)
			return nameResult;
		
		if (blkNum < blk.blkNum)
			return -1;
		else if (blkNum > blk.blkNum)
			return 1;
		
		return 0;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || !(obj.getClass().equals(BlockId.class)))
			return false;
		BlockId blk = (BlockId) obj;
		return fileName.equals(blk.fileName) && blkNum == blk.blkNum;
	}
	
	@Override
	public String toString() {
		return myString;
	}
	
	@Override
	public int hashCode() {
		return myHashCode;
	}
}
