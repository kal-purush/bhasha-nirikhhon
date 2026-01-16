package devTest;

public class Calc {

	public int add(int i, int j) {
		return i + j;
	}
	
	public int minus(int i, int j) {
		return i - j;
	}
	
	public int multi(int i , int j) {
		return i * j;
	}
	
	public int division(int i, int j) throws Exception {
		if(i == 0) {
			throw new Exception("0除算は行えません");
		}
		return i / j;
	}
}