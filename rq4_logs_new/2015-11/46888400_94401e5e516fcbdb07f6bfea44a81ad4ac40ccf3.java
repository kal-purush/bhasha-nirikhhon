package org.agnese.string;

class Calculator implements ICalculator {

	public int add(String numbers) {
		if (numbers == null || numbers.trim().isEmpty()) {
			return 0;
		}
		String[] addends = numbers.split(",");
		int result = 0;
		for (int i = 0; i < addends.length; i++) {
			result = result + Integer.parseInt(addends[i]);
		}
		return result;
	}

}