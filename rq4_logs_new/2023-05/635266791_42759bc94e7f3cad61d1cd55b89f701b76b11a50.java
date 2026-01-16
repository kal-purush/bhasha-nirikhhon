package BasicOops;

public abstract class CarParentAbstract {

	
	
	public void CarEngine()
	
	{
		System.out.println ("Car Engine");
		
	}
	
	public void CarSaftey()
	
	{
		System.out.println("Car Saftey");
	}
	
	// Abstract Method 

	public abstract void Carcolor();
}

//Abstract class may be abstract and non abstract method available
//non Abstract method must need a declaration [with body]
//Abstract method no need to declaration [without body]
//Interface contain 100% abstract method
// For interface use implements keyword
//For Abstract class use extends keyword