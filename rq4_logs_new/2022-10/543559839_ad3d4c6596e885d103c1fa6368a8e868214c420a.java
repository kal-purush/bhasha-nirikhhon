package QUESTION45;

import java.util.Scanner;

public class pattern_rhombus {
	public static void main(String[] args) {
		Scanner scn = new Scanner(System.in);
		int n = scn.nextInt();
		int row = 1;
		int  p = 1;
		int nmsp = n-1;
		int nmst = n-1;
		while(row<=2*n -1){
			int csp = 0;
			
			while(csp<nmsp) {
				System.out.print("0 ");
				csp++;
			}
			
			int cst = 1;
			while(cst<nmst) {
				System.out.print(p + " ");
				cst++;
				if(cst <= nmst / 2 ) {				p++;
				}else {
					p--;
				}
			}
			
			if(row<=n/2 +1) {
				nmst+=2;
				nmsp--;
				p++;
				
			}else {
				nmst-=2;
				nmsp++;
				p--;
			}
			row++;
			p++;
			System.out.println();
		}
	}
	
	

}