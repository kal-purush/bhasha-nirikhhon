//DisjointSet

public class DisjointSet {

	int[] rank, parent;
	int size;

	public DisjointSet(int size) {
		rank = new int[size];
		parent = new int[size];
		this.size = size;
		makeSet();
	}

	public void makeSet() {

		for(int i = 0; i < size; i++) {

			parent[i] = i;

		}

	}

	public int find(int x) {

		if(parent[x] != x) {
			parent[x] = find(parent[x]);
		}

		return parent[x];

	}

	public void union(int x, int y) {

		int xRep = find(x);
		int yRep = find(y);

		if(rank[xRep] < rank[yRep]) {
			parent[xRep] = yRep;
		}else if(rank[yRep] < rank[xRep]) {
			parent[yRep] = xRep;
		}else {
			parent[yRep] = xRep;
			rank[xRep] = rank[xRep] + 1;
		}

	}

}