package OOPJavaGB;

import java.util.Arrays;
import java.util.Comparator;

public class Lesson3Task1 {


	public static Integer[] MySort(int[] arr) {
		Integer[] result = new Integer[arr.length];
		for (int i = 0; i < arr.length; i++) {
			result[i] = arr[i] % 10;
		}

		Arrays.sort(result, new Comparator<Integer>() {
			@Override
			public int compare(Integer o1, Integer o2) {
			  return o1.compareTo(o2);
			}
		});
		for (int i = 0; i < result.length; i++) {
			for (int j = 0; j < arr.length; j++) {
				if(result[i] == arr[j] % 10){
					result[i] = arr[j];
				}
			}
		}
		return result;
	}

	public static void PrintResult(Integer[] arr){
		for (int i = 0; i < arr.length; i++) {
			System.out.println(arr[i].toString());
		}
	}
}