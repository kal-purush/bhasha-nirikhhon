package base1;

public class MyInteger {
	private int iValue; //stores the int value represented by this object

	//constructor
	public MyInteger(int iValue) {
		super();
		this.iValue = iValue;
	}
	
	//getter method
	public int getiValue() {
		return iValue;
	}
	
	//returns true iff iValue is even
	public boolean isEven() {
		if (iValue%2 == 0) 
			return true;
		else
			return false;
	}
	
	//returns true iff iValue is odd
	public boolean isOdd() {
		if (iValue%2 != 0)
			return true;
		else
			return false;
	}
	
	//returns true iff iValue is prime
	public boolean isPrime() {
		if (iValue%2 == 0)
			return false;
		for (int i=3;i*i<=iValue;i+=2) {
			if (iValue%i == 0)
				return false;
		}
		return true;
	}
	
	//returns true iff number (type int) is even
	public static boolean isEven(int number) {
		if (number%2 == 0) 
			return true;
		else
			return false;
	}
	
	//returns true iff number (type int) is odd
	public static boolean isOdd(int number) {
		if (number%2 != 0)
			return true;
		else
			return false;
	}
	
	//returns true iff number (type int) is prime
	public static boolean isPrime(int number) {
		if (number%2 == 0)
			return false;
		for (int i=3;i*i<=number;i+=2) {
			if (number%i == 0)
				return false;
		}
		return true;
	}

	//returns true iff MyInteger is even
	public static boolean isEven(MyInteger num1) {
		if (num1.getiValue()%2 == 0) 
			return true;
		else
			return false;
	}
	
	//returns true iff MyInteger is odd
	public static boolean isOdd(MyInteger num2) {
		if (num2.getiValue()%2 != 0) 
			return true;
		else
			return false;
	}
	
	//returns true iff MyInteger is prime
	public static boolean isPrime(MyInteger num3) {
		if (num3.getiValue()%2 == 0)
			return false;
		for (int i=3;i*i<=num3.getiValue();i+=2) {
			if (num3.getiValue()%i == 0)
				return false;
		}
		return true;
	}	
	
	// returns true if value in object is equal to specified value
	public boolean equals(int num4) {
		MyInteger INT = new MyInteger(num4);
		if (INT.getiValue() == num4)
			return true;
		else
			return false;
	}
	
	// returns true if value in object is equal to specified value
	public boolean equals(MyInteger num5) {
		MyInteger GB = new MyInteger(num5.getiValue());
		if (num5.getiValue() == GB.getiValue())
			return true;
		else
			return false;
	}
}