package POTD;

import java.util.HashMap;

public class KthDistinctStringInArray {
	public static void main(String[] args) {
		String[] arr = {"d","b","c","b","c","a"};
		System.out.println(kthDistinct(arr, 2));
	}

	public static String kthDistinct(String[] arr, int k) {
		HashMap<String, Integer> map = new HashMap<>();

		for(int i = 0; i < arr.length; i++) {
			map.put(arr[i], map.getOrDefault(arr[i], 0) + 1);
		}

		// System.out.println(map);

		for(int i = 0; i < arr.length; i++) {
			if(map.get(arr[i]) == 1) {
				if(k == 1) {
					return arr[i];
				}
				else {
					k--;
				}
			}
		}

		return "";
	}
}