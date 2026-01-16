import java.util.Scanner;


public class ArrayOfInts {
	public static void main(String[] args) {
		Scanner sc = new Scanner(System.in);
		System.out.println("Enter 10 positive integers bigger than 0:");
		int [] array = new int[10];
		int imax = 0;
		int imin = 0;
		for (int i = 0; i<array.length; i++) {
			int num = sc.nextInt();
			array[i] = num;
			/*for (int x = 0; x<num; x++) {
				System.out.print("*");
			}
			System.out.print("\n");*/
		}
	sc.close();
		int max = array[0];
		
		for (int i = 1; i<array.length; i++){
			if (array[i]>max){
				max = array[i];
				imax = i;
			}
		}
		int min = array[0];
		
		for (int i = 1; i<array.length; i++){
			if (array[i]<min){
				min = array[i];
				imin = i;
			}
		}
		array[imin] = max;
		array[imax] = min;
		
	//System.out.println(max + " is the maximum value, and " + min + " is the minimum value.");

	}
	
}