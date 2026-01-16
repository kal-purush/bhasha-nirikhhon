import java.io.*;
import java.lang.*;
public class Abc
{
	public static void main(String args[])
	{
		int a[][]=new int[2][10];
		char b[][]=new char[1][10];
		int th1,th2,tw1,tw2;
		Scanner scanin = new Scanner (System.in);
		for(int i=0;i<3;i++)
		{
			for(int j=0;j<10;j++)
			{
				if(i==0)
				{
				System.out.println("Enter student name");
				b[i][j]=scanin.nextChar();
				}
				if(i==1)
				{
					System.out.println("Enter student weight");
					a[i][j]=scanin.nextInt();
				}
				if(i==2)
				{
					System.out.println("Enter student height");
					a[i][j]=scanin.nextInt();
				}
			}
			
		}
	}
}
		
		