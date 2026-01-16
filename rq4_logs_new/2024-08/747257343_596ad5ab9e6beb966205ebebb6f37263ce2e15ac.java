package POTD;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class MinumumPushesToTypeWordII {
	public static void main(String[] args) {
		String word = "abcde";
		System.out.println(minimumPushes(word));
	}

	public static int minimumPushes(String word) {
		HashMap<Character, Integer> map = new HashMap<>();

		for(char ch : word.toCharArray()) {
			map.put(ch, map.getOrDefault(ch, 0) + 1);
		}

		List<Integer> lst = new ArrayList<>(map.values());
		Collections.sort(lst);

		int ans = 0;
		int elements = 1;
		for(int i = lst.size() - 1; i >= 0; i--) {
			if(elements <= 8) {
				ans += lst.get(i);
			}
			else if(elements >= 9 && elements <= 16) {
				int sum = lst.get(i) * 2;
				ans += sum;
			}
			else if(elements >= 17 && elements <= 24) {
				int sum = lst.get(i) * 3;
				ans += sum;
			}
			else {
				int sum = lst.get(i) * 4;
				ans += sum;
			}
			elements++;
		}
		return ans;
	}
}