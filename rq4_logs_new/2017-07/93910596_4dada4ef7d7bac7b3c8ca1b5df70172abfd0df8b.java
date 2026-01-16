package oops;

class Staff {
	void printNames()
	{
		System.out.println(" Ten Employees");
	}
}
class Teachers extends Staff
{
	void printNames()
	{
		System.out.println("Total Employees are 10");
		System.out.println(" Teaching staff 6 ");
		System.out.println(" Non - Teaching staff 4");
	}
	
	void child()
	{
	super.printNames();	
	}
	void display() 
	{
	System.out.println(" child display method");
	}
}
public class EmployeeEx4 {
	public static void main(String ar[]){
		
	Teachers t = new Teachers();
	t.child();
	t.display();
	
	}
}