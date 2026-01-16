package hearth;

import java.util.HashSet;
import java.util.Scanner;

public class practiceCode {

	public static void main(String[] args) {
		Scanner sc = new Scanner(System.in);
		int n = sc.nextInt();
		int count =0;
		int temp =0;
		for(int i=0;i<=Math.sqrt(n);i++) {
			for(int j=0;j<=Math.sqrt(n);j++) {
				temp = i*i+j*j;
				if(temp==n) {
					count++;
				}
			}
			
		}
		if(count>1) {
			System.out.println("yes");
		}else {
			System.out.println("no");
		}
    }
}