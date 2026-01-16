import java.util.Scanner;


public class P1RectangleArea {
	public static void main(String[] args) {
		Scanner scan= new Scanner (System.in);
		//getting the input
		String numbers= scan.nextLine();
		String[] strNum=numbers.split(" ");
		int num1=Integer.parseInt(strNum[0]);
		int num2=Integer.parseInt(strNum[1]);
		//finding the area
		int area=  num1 *num2;
		System.out.println("The area is : " + area);
		scan.close();
		//System.out.println(123);
	}
}