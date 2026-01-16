package pages;

public class C
{
	public static void main(String[] args) {
		
		int [] a= {22,11,10,44,55};
		int sum=0;
		
		for(int i=0; i<a.length;i++)
		{
			
			
			if(i%2==0)
			{
				
				System.out.println("Even index no. value is ="+a[i]);
				sum=sum+a[i];
				
				
								
			}
			
			
			
		}
	}

}