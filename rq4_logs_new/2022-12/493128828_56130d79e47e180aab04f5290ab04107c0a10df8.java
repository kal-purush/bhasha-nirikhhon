package arrays.programs;

public class ArrangeArrayElements {

	public static void main(String[] args) {
		// if n=9 output: 1 3 5 7 9 8 6 4 2
		int n = 9;
		int arr[]=new int[n];
		int val=1;
		int odd;
		if(n%2==0) {
			odd = n/2;
		}else {
			odd = (n/2)+1;
		}
		for(int i=0;i<odd;i++) {
			arr[i] = val;
			val+=2;
		}
		val=2;
		for(int j=n-1;j>=odd;j--) {
			arr[j]=val;
			val+=2;
		}
		for(int i=0;i<n;i++) {
			System.out.print(arr[i]+" ");
		}
	}

}