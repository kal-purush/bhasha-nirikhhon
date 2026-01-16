package study;

import java.util.LinkedList;

public class Calculator {

	String[] values;
	int result;

	LinkedList<Double> operands;
	LinkedList<String> operators;

	public void split(String param) {
		values = param.split(" ");

		if (values.length < 3) {
			throw new IllegalArgumentException("입력값이 올바르지 않습니다");
		}

		for (int i = 0; i < values.length; i++) {
			validate(i, values[i]);
			add(values[i]);
		}
	}

	private void validate(int idx, String param) {
		if (idx % 2 == 1 && isNumeric(param)) {
			throw new IllegalArgumentException("입력값이 올바르지 않습니다");
		} else if (idx % 2 == 0 && !isNumeric(param)) {
			throw new IllegalArgumentException("입력값이 올바르지 않습니다");
		}
	}

	private boolean isNumeric(String param) {
		return param.matches("[0-9]");
	}

	private void add(String param) {
		if (isNumeric(param)) {
			operands.add(Double.parseDouble(param));
		} else {
			operators.add(param);
		}
	}
}