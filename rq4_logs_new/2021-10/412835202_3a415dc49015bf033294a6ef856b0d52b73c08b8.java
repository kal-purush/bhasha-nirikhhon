class EvenOdd{
void checkevenodd(int n) {
		int sum1 = 0, sum2 = 0;
		while (n > 0) {
			int j = n % 10;
			if (j % 2 == 1) sum1 += j;
			else sum2 += j;
			n /= 10;
		}
		System.out.println("Odd sum is: " + sum1);
		System.out.println("Even sum is: " + sum2);
		if(sum1>sum2){
			System.out.println("Odd is larger");
		}
		else if(sum1<sum2){
			System.out.println("Even is larger");
		}
		else{
			System.out.println(" both is equal");
		}
		
	}
	
	public static void main(String[] args) {

		int n = 56783;
		EvenOdd t = new EvenOdd();
		t.checkevenodd(n);
	}
}