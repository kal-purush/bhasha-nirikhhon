package baekjoon.step11;

// ��Ʈ�λ��̵�
// ���ڸ� �����ϴ� ����

import java.util.Arrays;
import java.util.Collections;
import java.util.Scanner;

public class Sort_1427 {
	public static void main(String[] args) {
		@SuppressWarnings("resource")
		Scanner scanner = new Scanner(System.in);

		String N = scanner.nextLine();
		char[] array = N.toCharArray();

		sorting(array);

//		TestSort_1427.testCase();
	}

	static void sorting(char[] array) {
		// Arrays �޼ҵ�
		long start = System.nanoTime();
		System.out.println("Arrays �⺻����");
		char[] result1 = array;
		Arrays.sort(result1);
		for (int i = 0; i < result1.length; i++) {
			System.out.print(result1[i]);
		}
		System.out.println();
		long end = System.nanoTime();
		System.out.println((end-start)+" nano");

		System.out.println("Arrays �������� ����");
		Integer[] integerResult = new Integer[result1.length];

		for (int i = 0; i < integerResult.length; i++) {
			integerResult[i] = (int) result1[i] - 48;
		}

		Arrays.sort(integerResult, Collections.reverseOrder());
		for (int i = 0; i < integerResult.length; i++) {
			System.out.print(integerResult[i]);
		}
		System.out.println();

		// Counting Sort
		int[] intArray = new int[array.length];
		for (int i = 0; i < array.length; i++) {
			intArray[i] = (int) array[i] - 48;
		}

		// �迭 Ȯ��
		System.out.println("Counting �������� ����");
		for (int i = 0; i < intArray.length; i++) {
			System.out.print(intArray[i]);
		}
		System.out.println();

		int max = 0;
		for (int i = 0; i < intArray.length; i++) {
			if (intArray[i] > max) {
				max = intArray[i];
			}
		}

		int[] countArray = new int[max + 1];

		for (int i = 0; i < intArray.length; i++) {
			countArray[intArray[i]]++;
		}
		for (int i = 1; i < countArray.length; i++) {
			countArray[i] += countArray[i - 1];
		}

		int[] result2 = new int[intArray.length];
		for (int i = 0; i < intArray.length; i++) {
			int value = intArray[i];
			result2[countArray[value] - 1] = value;
			countArray[value]--;
		}

		int[] reverseResult2 = new int[result2.length];
		for (int i = reverseResult2.length - 1; i >= 0; i--) {
			reverseResult2[i] = result2[reverseResult2.length - 1 - i];
		}
		
		System.out.println("Counting �������� ����");
		for (int i = 0; i < reverseResult2.length; i++) {
			System.out.print(reverseResult2[i]);
		}

	}
}

class TestSort_1427 {
	static void testCase() {
		int M = (int) (Math.random() * 1000000000);
		String N = Integer.toString(M);
		char[] array = N.toCharArray();

		System.out.println("�Է°�: " + N);

		Sort_1427.sorting(array);

	}
}