package org.vanilladb.core.storage.tx.concurrency.tpl;

import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.tx.concurrency.ConcurrencyMgr;

public abstract class TwoPhaseLockingConcurrencyMgr implements ConcurrencyMgr {
	
	// Singleton
	protected static TplLockTable lockTbl = new TplLockTable();
	
	protected long txNum;

	/**
	 * Sets exclusive lock on the directory block when crabbing down for
	 * modification.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void crabDownDirBlockForModification(BlockId blk) {
		lockTbl.xLock(blk, txNum);
	}

	/**
	 * Sets shared lock on the directory block when crabbing down for read.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void crabDownDirBlockForRead(BlockId blk) {
		lockTbl.sLock(blk, txNum);
	}

	/**
	 * Releases exclusive locks on the directory block for crabbing back.
	 * 
	 * @param blk
	 *            the block id	
	 */
	public void crabBackDirBlockForModification(BlockId blk) {
		lockTbl.release(blk, txNum, TplLockTable.X_LOCK);
	}

	/**
	 * Releases shared locks on the directory block for crabbing back.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void crabBackDirBlockForRead(BlockId blk) {
		lockTbl.release(blk, txNum, TplLockTable.S_LOCK);
	}

	public void lockRecordFileHeader(BlockId blk) {
		lockTbl.xLock(blk, txNum);
	}

	public void releaseRecordFileHeader(BlockId blk) {
		lockTbl.release(blk, txNum, TplLockTable.X_LOCK);
	}
}
