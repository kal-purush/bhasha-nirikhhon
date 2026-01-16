import java.io.*;
import java.util.*;

public class Main {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        int rows = sc.nextInt();
		int cols = sc.nextInt();		
		int cats = sc.nextInt();
		int array[][] = new int [rows+1][cols+1];
		boolean cat[][] = new boolean [rows+1][cols+1];
		for (int k = 0; k < cats; k++) {
			int y = sc.nextInt();
			int x = sc.nextInt();
			cat[y][x] = true;
		}
		array[1][1] = 1;
		for (int r = 1; r < rows + 1; r++) {
			for (int c = 1; c < cols + 1; c++) {
				if ((r != 1 || c != 1) && cat[r][c] == false) {
					if (cat[r-1][c] && cat[r][c-1]) array[r][c] = 0;
					else if (cat[r-1][c]) array[r][c] = array[r][c-1];
					else if (cat[r][c-1]) array[r][c] = array[r-1][c];
					else array[r][c] = array[r-1][c] + array[r][c-1];
				}
			}
		}
		System.out.println(array[rows][cols]);

    }
}