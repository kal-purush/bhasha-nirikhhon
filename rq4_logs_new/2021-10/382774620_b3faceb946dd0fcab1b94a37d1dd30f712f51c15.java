
public class Pattern1 {
	public static void main(String args[]) {
		int rowCount = 5;
		int columnsCount = 5;
		for(int row = 1; row<=rowCount; row++) {
			for(int column = 1; column<=row; column++) {
				System.out.print(column + " ");
			}
			System.out.println(" ");
		}
	}

}