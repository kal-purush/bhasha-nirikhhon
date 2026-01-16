package pl.edu.agh.kis;

import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Iterator;
import java.util.PriorityQueue;

import Interfaces.Cache;

public class NanoCache implements Cache {
	private HashMap<Integer, BigInteger> storage = new HashMap<Integer, BigInteger>();
	private PriorityQueue<NanoIndex> deletionQueue = new PriorityQueue<NanoIndex>();

	int storageSupremum = 4000;
	int currentSize = 0;

	public NanoCache(int storageSupremum) {
		this.storageSupremum = storageSupremum;

		storage.put(new Integer(0), new BigInteger("" + 1));
		currentSize = 1;
	}

	@Override
	public void storeResult(int index, BigInteger result) {
		if (isFull()) {
			System.out.println("Cache is full! Removing least used element");
			deleteLeastUsedResult();
			storeResult(index, result);
		}

		if (!containsIndex(index)) {
			storage.put(index, result);
			currentSize++;
			deletionQueue.add(new NanoIndex(index, System.nanoTime()));
		} else {
			deleteNanoIndexFromDeletionQueue(index); // Putting new nanoIndex on
														// the tail
			deletionQueue.add(new NanoIndex(index, System.nanoTime()));
		}
	}

	@Override
	public BigInteger getResult(int index) {
		BigInteger result = storage.get(index);

		deleteNanoIndexFromDeletionQueue(index); // Putting new nanoIndex on the
													// tail
		deletionQueue.offer(new NanoIndex(index, System.nanoTime()));

		return result;
	}

	@Override
	public boolean containsIndex(int index) {
		return storage.containsKey(index);
	}

	@Override
	public boolean isEmpty() {
		return (currentSize == 0);
	}

	@Override
	public boolean isFull() {
		return (evaluateMemoryUsage() > storageSupremum);
	}

	@Override
	public void clearCache() {
		storage = new HashMap<Integer, BigInteger>(storageSupremum);
		storage.put(new Integer(0), new BigInteger("" + 1));
		currentSize = 1;
	}

	public long evaluateMemoryUsage() {
		long suma = 0;

		for (BigInteger bigInt : storage.values()) {
			try {
				suma += Utility.sizeof(bigInt);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return suma;
	}

	public void deleteLeastUsedResult() {
		NanoIndex oldestNanoIndex = deletionQueue.poll();
		int oldestIndex = oldestNanoIndex.getIndex();

		if (storage.containsKey(oldestIndex)) {
			System.out.println("Removing result of index: " + oldestIndex);
			storage.remove(oldestIndex);
			--currentSize;
		} else {
			System.out.println("Tried removing result of index: " + oldestIndex + " But it was not in storage");
		}
	}

	public void deleteNanoIndexFromDeletionQueue(int index) {
		/*for (NanoIndex nano : deletionQueue) {
			if (nano.getIndex() == index) {
				deletionQueue.remove(nano);
			}
		}*/
		
		Iterator it = deletionQueue.iterator();
		while (it.hasNext()){
			NanoIndex nano = (NanoIndex)(it.next());
			if (nano.getIndex() == index) {
				it.remove();
			}
		}
	}

}

class NanoIndex implements Comparable<NanoIndex> {
	int index;
	long nano;

	public NanoIndex(int index, long nano) {
		super();
		this.index = index;
		this.nano = nano;
	}

	public int getIndex() {
		return index;
	}

	public long getNano() {
		return nano;
	}

	@Override
	public int compareTo(NanoIndex ex) {
		if (this.getNano() < ex.getNano()) {
			return -1;
		} else if (this.getNano() == ex.getNano()) {
			return 0;
		} else {
			return 1;
		}
	}

	@Override
	public String toString() {
		return (index + "");
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof NanoIndex) {
			NanoIndex c = (NanoIndex) o;
			return index == c.index;
		}
		return false;
	}
}