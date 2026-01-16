package Contest.Biweekly136;

import java.util.HashMap;
import java.util.Map;

public class FindTheNumberOfWinningPlayers {
	public static void main(String[] args) {
		int[][] pick = {
			{0, 0},
			{1, 0},
			{1, 0},
			{2, 1},
			{2, 1},
			{2, 0}
		};

		System.out.println(winningPlayerCount(4, pick));
	}

	public static int winningPlayerCount(int n, int[][] pick) {
		int ans = 0;

		for(int i = 0; i < n; i++) {
			HashMap<Integer, Integer> mp = new HashMap<>();

			for(int[] p : pick) {
				if(p[0] == i) {
					mp.put(p[1], mp.getOrDefault(p[1], 0) + 1);
				}
			}

			for(Map.Entry<Integer, Integer> map : mp.entrySet()) {
				// System.out.println(map.getKey() + " " + i);
				if(map.getValue() > i) {
					ans++;
					break;
				}
			}
		}

		return ans;
	}
}