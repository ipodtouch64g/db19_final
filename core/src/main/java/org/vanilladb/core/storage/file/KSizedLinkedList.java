package org.vanilladb.core.storage.file;

import java.util.LinkedList;

public class KSizedLinkedList<E> extends LinkedList<E> {
	
	private int limit;
	public KSizedLinkedList(int limit)
	{
		this.limit = limit;
	}
	@Override
	public boolean add(E o)
	{
		super.add(o);
		while(size() > limit)
			super.remove();
		return true;
	}

}
