import java.util.*;

import java.io.*;

public class Solution {

	public static ArrayList<ArrayList<Integer>> findTriplets(int[] arr, int n, int k) {

		// Write your code here.
		Arrays.sort(arr);
		int x, y, z;
		ArrayList<ArrayList<Integer>> list = new ArrayList<>();
		for (x = 0; x < n-1; x++) {
			
			if(x>0 && arr[x-1]==arr[x])
			continue;
			y = x + 1;
			z = n - 1;
			while (y < z) {
				int sum = arr[x] + arr[y] + arr[z];
				if (sum < k)
					y++;
				else if (sum > k) {
					z--;
				} else {
					ArrayList<Integer> temp = new ArrayList<>();
					temp.add(arr[x]);
					temp.add(arr[y]);
					temp.add(arr[z]);
					list.add(new ArrayList<>(temp));
					y++;
					z--;
					while(y<n && arr[y]==arr[y-1])
					y++;
				}
			}
		}
		return list;
	}
}