package com.flouis.test;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MyTest {

	@Test
	public void MainTest(){
		Integer[] arr = {36, 28, 45, 13, 67, 37, 18, 56};
		/*bubbleSort(arr);
		for (int x : arr){
			System.out.print(x+" ");
		}*/
		List<Integer> list = new ArrayList<>(Arrays.asList(arr));
		list = bubbleSort(list);
		for (int x : list){
			System.out.print(x + " ");
		}

	}



	public static List bubbleSort(List<Integer> list){
		int temp;
		Integer[] arr = (Integer[]) list.toArray(new Integer[0]);
		// 做多少轮排序（最多length-1轮）
		for (int i = 0; i < arr.length - 1; i++) {
			// 每一轮比较多少个
			for (int j = 0; j < arr.length - 1 - i; j++) {
				if (arr[j] > arr[j + 1]) {
					// 交换次序
					temp = arr[j];
					arr[j] = arr[j + 1];
					arr[j + 1] = temp;
				}
			}
		}

		return Arrays.asList(arr);
	}

	/**
	 * 冒泡排序算法
	 */
	public static void bubbleSort(Integer[] list) {
		int temp;
		// 做多少轮排序（最多length-1轮）
		for (int i = 0; i < list.length - 1; i++) {
			// 每一轮比较多少个
			for (int j = 0; j < list.length - 1 - i; j++) {
				if (list[j] > list[j + 1]) {
					// 交换次序
					temp = list[j];
					list[j] = list[j + 1];
					list[j + 1] = temp;
				}
			}
		}
	}

}