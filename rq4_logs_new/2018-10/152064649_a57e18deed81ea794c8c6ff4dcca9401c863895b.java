public class MCM {
	private static int matrices = 0;
	private static String order = "";
	
	public static void main(String[] args) {
		
		// -------------- EDIT THE MATRIX DIM ONLY --------------
					int[] dim = {30, 35, 15, 5, 10, 20, 25};
		// -------------- EDIT THE MATRIX DIM ONLY --------------
		
		int [][] fin = Matrix_Chain_Order(dim);
		matrices = dim.length - 2;
		order = getAns(fin, 1, fin[0].length+1);
		order = order.substring(1,  order.length()-1);
		System.out.println(order);
	}
	
	private static int [][] Matrix_Chain_Order(int[] dim){
		int x = dim.length-1;
		int mat[][] = new int[x][x];
		int sol[][] = new int[x-1][x-1];
		
		for(int i = 0; i < x; i++) {
			mat[i][i] = 0;
		}
		
		for(int l = 1; l < x; l++) {
			int k = l-1;
			while(k >= 0) {
				int i = l;
				int j = l-1;
				int val = Integer.MAX_VALUE;
				int min = Integer.MAX_VALUE;
				int min_pos = 0;
				do {	
					if(i == l) {
						min = mat[i][l]+mat[k][j]+dim[k]*dim[l+1]*dim[i];
						min_pos = j+1;
					}else {
						val = mat[i][l]+mat[k][j]+dim[k]*dim[l+1]*dim[i];
					}
					if(k == l-1) {
						break;
					}
					if(min > val) {
						min = val;
						min_pos = j+1;
					}
					i--;
					j--;
				}while(k != i);
				mat[k][l] = min;
				sol[k][l-1] = min_pos;
				k--;
			}
		}
		printMatrix(mat);
		printMatrix(sol);
		System.out.println("Minimum matrix: " + mat[0][x-1]);
		return sol;
	}
	
	private static void printRow(int num) {
		for(int i = 0; i < num; i++) {
			int val = i+1;
			System.out.print("[" + val + "]\t");
		}
		newLine();
	}

	private static void newLine() {
		System.out.println();
	}
	
	private static void printBracket(int val) {
		System.out.println("[" + val + "]");
	}
	
	private static void printMatrix(int[][] mcm) {
		printRow(mcm.length);
		int count = 1;
		for(int[] i: mcm) {
			for(int j: i) {
				System.out.print(j + "\t");
			}
			printBracket(count);
			count++;
		}
		newLine();
	}
	// [30, 35, 15, 5, 10, 20, 25]
	
	private static String getAns(int[][] ans, int start, int end) {
		if(start == end) {
			return "A" + (start);
		}else if(start == end-1) {
			return ("(A" + (start) + " A" + (end) +")"); 
		}
		int pos = ans[start-1][end-2];
		String temp = "(" + getAns(ans, start, pos) + getAns(ans, pos+1, end) + ")";
		return temp;
	}
}