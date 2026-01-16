import university.Professor;
import collections.Student;


public class StackAndHeapTest {

	String abcd;
	
	public void method1(Student s1 , int x){
		int c = 100;
		int z = c + 1;
		
		System.out.println(x);
		method2(s1);
		//method4
		
	}
	
	public void method2(Student s2){
		int c = 200;
		Professor p1 = new Professor(); 
		System.out.println(s2);	
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		final Student student = new Student(300, "Name 3");
		System.out.println(student.getId());
		student.setId(400);
		
		//student = new Student();
		
		System.out.println(student.getId());
		
		int y = 100;
		
		final int x = y;
		
		//x = x + 1;
		
		StackAndHeapTest test = new StackAndHeapTest();
		
		test.method1(student, 100);
		
	}

}