package src.ru.avdotchenkov.lesson6;

import java.util.Arrays;

public class ArrayMy {
	public static void main (String[] args) {
		int [] a = {1,2,3,4,5,6,7};
		System.out.println(Arrays.toString(a));
		System.out.println(Arrays.toString(arrayNOffset(a, -2)));
	}
	
	
	static int[] arrayNOffset (int[] array, int n) {
		
		if (n < 0) {
			negativeArrayNOffset(array, n);
		} else {
			positiveArrayNOffset(array, n);
		}
		
		
		return array;
	}
	
	private static int[] negativeArrayNOffset (int[] array, int n) {
		int length = array.length;
		int current;
		int next;
		n *= - 1;
		
		for (int i = 0; i < n; i++) {
			for (int k = 0; k < length - 1; k++) {
				current = array[k];
				next = k + 1;
				
				if (k + 1 > length - 1) {
					next = 0;
				}
				array[k] = array[next];
				array[next] = current;
			}
		}
		
		return array;
	}
	
	private static int[] positiveArrayNOffset (int[] array, int n) {
		int length = array.length - 1;
		int current;
		int prev;
		
		for (int i = 0; i < n; i++) {
			for (int k = length; 0 <= k; k--) {
				current = array[k];
				prev = k - 1;
				
				if (k - 1 < 0) {
					prev = 0;
				}
				array[k] = array[prev];
				array[prev] = current;
			}
			
		}
		
		return array;
	}
	
}