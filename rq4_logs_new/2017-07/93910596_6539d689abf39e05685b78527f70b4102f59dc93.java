package oops;

class Dental
{
	Dental()  //default Constructor
	{
		System.out.println("Inside Constructor");
	}
	Dental(int x) //parameterized Constructor
	{
		System.out.println(" x value = "+x);
	}
	Dental(int x, int y) //parameterized Constructor
	{
		System.out.println(" y value= "+y);
	}
	void location() //parameterized Constructor
	{
		System.out.println("California");
	}
}
public class Hospital {

	public static void main(String[] args) {
Dental d1 = new Dental(5,10); //Actual Parameter
d1.location();

	}

}