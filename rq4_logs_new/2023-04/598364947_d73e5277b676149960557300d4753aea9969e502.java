package L07;

public class Ex01 {

	public static void main(String[] args) {
		int x = 0;	//assume we read a value from user
		compute(x);
	}

	private static void compute(int x) {
		if(x!=0)
			System.out.println(6/x);
		else
			System.out.println("Cannot divide by 0");
	}
}