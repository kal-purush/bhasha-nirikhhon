package sample;

public class main2 {

	public static void main(String[] args) {
		int arry[][] = {{1, 2, 3 ,4}, { 5, 6, 7}};
		int sum = 0;
		for (int i = 0; i < arry.length; i++) {
			for (int j = 0; j < arry[i].length; j++) {
				sum += arry[i][j];
			}
		}
		System.out.println("summary1:" + sum);
		int arry2[] = arry[1];
		int sum2 = 1;
		for (int i = 0; i < arry2.length; i++) {
			sum2 *= arry2[i]; 
		}
		System.out.println("summary2:" + sum2);
		int arry3[] = arry[0];
		int sum3 = 0;
		for (int i = 0; i < arry3.length; i++) {
			if (arry3[i] %2 == 0) {
				System.out.println("Item:" + i + ",Value:" + arry3[i]);
				sum3 += arry3[i];
			}
		}
		System.out.println("summary3:" + sum3);
	}

}