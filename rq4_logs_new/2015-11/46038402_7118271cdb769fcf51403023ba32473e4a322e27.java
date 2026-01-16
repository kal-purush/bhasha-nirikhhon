package firstPage;

import java.util.Scanner;

public class CounterfeitDollar1013 {

	public static void main(String[] args) {
		Scanner in = new Scanner(System.in);
		int n=Integer.parseInt(in.nextLine());
		String result[]=new String[n];
		int count=0;
		while(count<n){
			int coins[]=new int[12];
			boolean isTrue[]=new boolean[12];
			for(int i=0;i<3;i++){
				String temps[]=in.nextLine().split(" ");
				char left[]=temps[0].toCharArray();
				char right[]=temps[1].toCharArray();
				if(temps[2].equals("even")){
					for(int j=0;j<left.length;j++){
						isTrue[left[j]-65]=true;
						isTrue[right[j]-65]=true;
					}
				}else if(temps[2].equals("up")){
					for(int j=0;j<left.length;j++){
						++coins[left[j]-65];
						--coins[right[j]-65];
					}
				}else{
					for(int j=0;j<left.length;j++){
						--coins[left[j]-65];
						++coins[right[j]-65];
					}
				}
			}
			int max=0;
			int loc=0;
			for(int i=0;i<12;i++){
				if(isTrue[i]==false&&Math.abs(coins[i])>max){
					max=Math.abs(coins[i]);
					loc=i;
				}
			}
			String temp="";
			if(coins[loc]>0) 
				temp="heavy.";
			else 
				temp="light.";
			result[count]=(char)(loc+65)+" is the counterfeit coin and it is " +temp;
			++count;
		}
		for(int i=0;i<n;i++)
			System.out.println(result[i]);
	}

}
