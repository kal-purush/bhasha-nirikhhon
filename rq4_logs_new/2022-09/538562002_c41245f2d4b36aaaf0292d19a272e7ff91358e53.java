import java.util.*;

/*Question1 : print patterns using loops 
  Question2 : print multiplication table */

public class patternTable {

	public static void main(String[] args) {
		patternTable obj=new patternTable(); 
			obj.pattern1();
			obj.pattern2();
			obj.pattern3();
			obj.pattern4();
			obj.table1();
			obj.table2();
		}
		public void pattern1()
		{
			
			int i,j, alpha = 65; //ASCII value of capital A is 65   
			for (i=0;i<=5;i++)  
			{        
				for (j=0;j<i;j++)  
				{  
					System.out.print((char) (alpha + j) + " ");   
				}  
				System.out.println();  
			}  
		}
		
		public void pattern2()
		{
			Scanner s = new Scanner(System.in);
			int i,j,size;
			System.out.println("\n Enter size:");
			size = s.nextInt();
			
			for(i=1;i<=size;i++)
			{
				for(j=i;j<size;j++)
				{
					System.out.print(" ");
				}	
				for(j=1;j<=i;j++)
				{
					System.out.print(j);
				}
				System.out.println();
			}
		}
		
		public void pattern3()
		{
			Scanner c=new Scanner(System.in);
			int i,j,size;
			System.out.println("\n Enter size:");
			size = c.nextInt();
			for(i=1;i<=size;i++)
			{
				for(j=i;j<size;j++)
				{
					System.out.print(" ");
				}	
				for(j=1;j<=(2*i-1);j++)
				{
					System.out.print("*");
				}
				System.out.println();
			}
		}
		
		public void pattern4()
		{
			Scanner sc=new Scanner(System.in);
			int size,i,j,p;
			char c;
			System.out.println("Enter size:");
			size = sc.nextInt();
			for(i=1;i<=size;i++)
			{
				for(j=1;j<i;j++)
				{
					System.out.print(" ");
				}
				for( p=i;p<=size;p++)
				{
					if(p%2==0)
					{
						System.out.print("0");
					}
					else
					{
					System.out.print("1");
					}
				}
				System.out.println();
			}
		}
		
		// print multiplication table  
		public void table1()
		{
			Scanner sc = new Scanner(System.in);  
			System.out.print("Enter number: ");       
			int num=sc.nextInt(); 
			for(int i=1; i <= 10; i++)  
			{  
			System.out.println(num+" * "+i+" = "+num*i);  
			}   
		}
		public void table2()
		{
			System.out.println("The Table from 1 to 10: \n");
			for (int i = 1; i <= 10; i++) 
			{
				for (int j = 1; j <= 10; j++) 
				{
					System.out.print(j + "*" + i + "=" + i * j + "\t");
				}
				System.out.println();
			}
	}

}