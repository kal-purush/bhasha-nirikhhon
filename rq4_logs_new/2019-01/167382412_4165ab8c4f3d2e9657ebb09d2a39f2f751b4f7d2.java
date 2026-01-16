
public class ConversionProject 
{

	public static void main(String[] args) 
	{
		System.out.println(feetToInches(8));
		System.out.println(milesToFeet(2));
	}

	public static double feetToInches(double feet)
	{
		double inches = 12*feet;
		return inches;	
	}
	public static double milesToFeet(double miles)
	{
		double milesfeet = 5280*miles;
		return milesfeet;
	}
}