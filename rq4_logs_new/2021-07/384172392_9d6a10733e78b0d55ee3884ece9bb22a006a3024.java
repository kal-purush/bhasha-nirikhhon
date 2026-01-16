package com.bridgelabz.maximum;

public class Maximum<E extends Comparable<E>> {
	E firstValue;
	E secondValue;
	E thirdValue;

	public Maximum(E firstValue, E secondValue, E thirdValue) {
		this.firstValue = firstValue;
		this.secondValue = secondValue;
		this.thirdValue = thirdValue;
	}

	public static void main(String[] args) {
		System.out.println(new Maximum(1.2f,4.2f,5.6f).getMax());
		System.out.println(new Maximum(40,10,90).getMax());
		System.out.println(new Maximum("apple", "banana", "peach").getMax());
	}

	public static <E extends Comparable<E>> E getMax(E firstValue, E secondValue, E thirdValue) {
		E max = firstValue;
		if (secondValue.compareTo(max) > 0)
			max = secondValue;
		if (thirdValue.compareTo(max) > 0)
			max = thirdValue;
		return max;
	}

	public <E extends Comparable<E>> E getMax() {
		E max = (E) getMax(firstValue, secondValue, thirdValue);
		return max;
	}
}