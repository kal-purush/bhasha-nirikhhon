

package Interfaces;
import java.util.Scanner;

interface marks
{
   public int getMarks();
}
interface sports
{
	int sportmark = 20;
   public void getSports();
}




class Student implements marks,sports
{
	
	
	int total;
	Scanner sc = new Scanner (System.in);
  @Override	
   public int getMarks()
   {
        System.out.println("Enter maths marks");
        
        int maths = 0;
		try {
			maths = sc.nextInt();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("Enter a number");
		}
        System.out.println("Enter science marks");
        int science = sc.nextInt();
        System.out.println("Total is " + (science+maths));
        total = science + maths;
        return total;
   }
  static int finalmarks;
   public static void main(String args[])
   {
        Student sc1 = new Student();
         finalmarks =  sc1.getMarks();
        sc1.getSports();
    }
@Override
public void getSports() {
  System.out.println("Total marks including the sports is  " + ( finalmarks+ sportmark));	
}
}