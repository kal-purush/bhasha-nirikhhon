import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class A {

	private int[] vertices;
	private boolean[][] edges;
	private int edgeCount;
	
	public A(File file) throws IOException {
		readGraph(file);
	}

	private void readGraph(File file) throws FileNotFoundException, IOException {
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line;
		while ((line = br.readLine()) != null) {
			if (line.startsWith("c")) continue;
			
			String[] split = line.split("\\s+");
			if (line.startsWith("p")) {
				int vertexCount = Integer.parseInt(split[2]);
				int edgeCount = Integer.parseInt(split[3]);
				this.vertices = new int[vertexCount];
				this.edges = new boolean[vertexCount][vertexCount];
				this.edgeCount = edgeCount;
			}
			else {
				int left = Integer.parseInt(split[1]);
				int right = Integer.parseInt(split[2]);
				this.edges[left][right] = true;
				this.edges[right][left] = true;
			}
		}
		br.close();
	}
	
	public int[] getVertices() {
		return this.vertices;
	}

	public boolean[][] getEdges() {
		return this.edges;
	}
}