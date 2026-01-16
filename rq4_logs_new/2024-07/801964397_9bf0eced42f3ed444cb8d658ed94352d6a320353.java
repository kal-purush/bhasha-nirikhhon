import java.io.*;
import java.util.*;

public class code17503 {
	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		StringTokenizer st = new StringTokenizer(br.readLine());
		int n = Integer.parseInt(st.nextToken());
		int m = Integer.parseInt(st.nextToken());
		int k = Integer.parseInt(st.nextToken());

		Queue<Integer> prefers = new PriorityQueue<>();
		List<Beer> beers = new ArrayList<>();
		for (int i = 0; i < k; i++) {
			st = new StringTokenizer(br.readLine());
			int v = Integer.parseInt(st.nextToken());
			int c = Integer.parseInt(st.nextToken());
			beers.add(new Beer(v, c));
		}
		Collections.sort(beers);

		int total = 0;
		int answer = -1;
		for (Beer beer : beers) {
			prefers.add(beer.v);
			total += beer.v;

			if (prefers.size() > n) {
				// n병 이상이라면
				total -= prefers.poll();
			}
			if (prefers.size() == n && total >= m) {
				answer = beer.c;
				break;
			}
		}
		System.out.print(answer);
	}

}
class Beer implements Comparable<Beer> {
	int v;
	int c;

	Beer(int v, int c) {
		this.v = v;
		this.c = c;
	}

	@Override
	public int compareTo(Beer o1) {
		if (this.c == o1.c) {
			return o1.v - this.v;
		}
		return this.c - o1.c;
	}
}
