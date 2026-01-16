package com.kwords.heap;

import java.util.Comparator;
import java.util.PriorityQueue;

public class MinHeap extends PriorityQueue<HeapNode>{

	private static final long serialVersionUID = 3539915569208279149L;
	private Integer capacity = null;
	
	public MinHeap(Integer capacity){
		super(capacity, new Comparator<HeapNode>() {
	        @Override
	        public int compare(HeapNode o1, HeapNode o2) {
	            return o1.getCount()-o2.getCount();
	        }
	    });
		this.capacity = capacity;
	}

	@Override
	public boolean add(HeapNode e) {
		//TODO: capacity logic here	
		return super.add(e);
	}
	
}