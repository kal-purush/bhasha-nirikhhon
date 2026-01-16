package com.ubs.sai;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;


public class SimpleHashTableImpl<K, V> implements SimpleHashTable<K, V> {
	private static final int DEF_SIZE = 100;
	private List<List<Entry<K, V>>> entries = new ArrayList<List<Entry<K, V>>>();

	public SimpleHashTableImpl() {
		super();
		init();
	}
	private void init() {
		for(int i=0;i<DEF_SIZE;i++) {
			entries.add(new LinkedList<>());
		}
	}
	@Override
	public V get(K key) {
		int loc = key.hashCode() % DEF_SIZE;		
		Optional<Entry<K, V>> entry =
		entries.get(loc)
		.stream()
		.filter(entr -> entr.key.equals(key))
		.findFirst();
		
		return entry.isPresent() ? entry.get().getValue():null;
	}

	@Override
	public void put(K key, V value) {
		int loc = key.hashCode() % DEF_SIZE;
		entries.get(loc).add(new Entry<>(key, value));
	}
	
	public static class Entry<K, V>{
		private final K key;
		private final V value;
		public Entry(K key, V value) {
			super();
			this.key = key;
			this.value = value;
		}
		public K getKey() {
			return key;
		}
		public V getValue() {
			return value;
		}		
	}	
}