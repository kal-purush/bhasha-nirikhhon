package midtermexam;

public class Calculator {
	double sum =0;
	double diff=0;
	double product=0;
	int div=0;
	
	double sum(double num1, double num2)
	{
		sum=num1+num2;
		return sum;
	}
	
	double difference(double num1, double num2)
	{
		diff= num1-num2;
		return diff;
	}
	double multiplication(double num1, double num2)
	{
		product=num1*num2;
		return product;
	}
	int division(int num1, int num2)
	{
		try
		{
		 div=num1/num2;
		
		return div;
		}
		catch (ArithmeticException e)
		{
			System.out.println("Exception Occurred: "+e);
		}
		return num2;
	}

}
