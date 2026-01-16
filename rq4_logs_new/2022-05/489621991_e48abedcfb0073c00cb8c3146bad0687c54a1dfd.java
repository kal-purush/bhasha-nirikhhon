import java.util.Scanner;
class Tech{
	public static void main(String[] args) {
	Scanner sn=new Scanner(System.in);
	int n;
	System.out.println("Enter Number of Employees:");
	n=sn.nextInt();
	Teacher tech[]=new Teacher[n];
	for (int i=0;i<n;i++) {
		tech[i]=new Teacher();
	}
	for (int i=0;i<n;i++) {
		System.out.println("\nDetails of Teacher:");
		tech[i].display();
	}
	
}
}

class Employee{
	Scanner sn=new Scanner(System.in);
	int eid;
	String ename;
	float salary;
	String addr;

	Employee(){
		System.out.println("\nEnter Details of Teacher:");
		System.out.println("Enter employee id:");
		eid=sn.nextInt();
		System.out.println("Enter employee name:");
		ename=sn.next();
		System.out.println("Enter Salary:");
		salary=sn.nextFloat();
		System.out.println("Enter Address:");
		addr=sn.next();
	}
}
class Teacher extends Employee{
	String dept;
	String sub;
	Teacher(){
		super();
		System.out.println("Enter Department:");
		dept=sn.next();
		System.out.println("Enter Subject:");
		sub=sn.next();
	}

	void display(){
		System.out.println("eid:"+eid);
		System.out.println("Name:"+ename);
		System.out.println("Salary:"+salary);
		System.out.println("Address:"+addr);
		System.out.println("Department:"+dept);
		System.out.println("Subject:"+sub);
	}
}