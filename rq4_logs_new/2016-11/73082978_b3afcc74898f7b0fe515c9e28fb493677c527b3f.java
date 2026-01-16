package Interfaces;

import java.math.BigInteger;

public interface Cache {
	public void storeResult(int index, BigInteger result);
	public BigInteger getResult(int index);
	public boolean containsIndex(int index);
	public boolean isEmpty();
	public boolean isFull();
	public void clearCache();
	//public BigInteger getNearestResultBelowGivenIndex(int index);
	//public BigInteger calculateFactorialReusingResult(int n);
	//public BigInteger calculateFactorialReusingResult(int index, int n, BigInteger storedResult);
}