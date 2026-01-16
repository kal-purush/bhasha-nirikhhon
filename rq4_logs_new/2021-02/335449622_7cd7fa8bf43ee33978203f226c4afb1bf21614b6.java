import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Scanner;

public class GroupProject {

	public static void main(String[] args) throws FileNotFoundException {
		// TODO Auto-generated method stub
		
		Scanner sc = new Scanner(new File("inventory_team6.csv"));
		sc.useDelimiter(",");
		String[][] data = new String[42776][6];
		int i = 0;
		int j = 0;
		while (sc.hasNext()) {
			j = 0;
			i++;

			while (j < 5) {
				j++;
				data[i][j] = sc.next();
			}
		}
		System.out.println( Arrays.deepToString(data));
	}
}