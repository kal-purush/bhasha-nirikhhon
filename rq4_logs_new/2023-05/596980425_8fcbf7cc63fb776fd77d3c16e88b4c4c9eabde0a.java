package programmers.level.one;

import java.util.Arrays;
import java.util.Comparator;

public class Solution73 {
	// 가장 큰 수
	public String solution(int[] numbers) {
		String[] stringNumbers = new String[numbers.length];

		// String형 배열로 바꾸기
		for (int i = 0; i < numbers.length; i++) {
			stringNumbers[i] = Integer.toString(numbers[i]);
		}

		// 두 수를 번갈아가며 합친 수 중 큰 값을 앞으로 정렬하기
		Arrays.sort(stringNumbers, new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) { 
				return (o2 + o1).compareTo(o1 + o2);  // 내림차순 정렬
			}
		});

		if (stringNumbers[0].equals("0"))
			return "0";

		// 문자열을 delimiter("") 기준으로 합침
		return String.join("", stringNumbers);
	}

}