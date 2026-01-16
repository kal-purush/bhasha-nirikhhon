package oops;

class Chase {
	
	int eid;
	String ename;
	double sal;   // fields
	
	void setDetails()  // No Arguments 
	{
		eid=101;
		ename="raj";
		sal=5000.0;
		
	}
	
	void getDetails()
	{
		System.out.println(" Eid = "+eid);
		System.out.println("Employee name = "+ename);
		System.out.println("Employee salary = "+sal);
	}

}
public class Employee
{
	public static void main(String ar[])
	{
		Chase e1 = new Chase();
		Chase e2 = new Chase();
		e1.setDetails();
		e2.setDetails();
		
		e1.getDetails();
		e2.getDetails();
		
	}
}
