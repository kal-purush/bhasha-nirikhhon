
public class MinArrayNumber {

	public static void main(String[] args) 
	
	{
		
		
		minvalue();
		Maxvalue();
		
		
	}
	
	public static void minvalue()
	{
		int [][] abc = {{1,2,3}, {4,5,6}, {7,8,9}};
		int min = abc[0][0];
		
		for (int i=0;i<3;i++)
		{
			for(int j=0;j<3;j++)
			{
				if(abc[i][j]<min)
				{
					min = abc[i][j];
					
				}
			}
			
		}
		System.out.println(min);
	}
	
	public static void Maxvalue()
	{
		int [][] abc = {{1,2,3}, {4,5,6}, {7,8,9}};
		int max = abc[0][0];
		
		for (int i=0;i<3;i++)
		{
			for(int j=0;j<3;j++)
			{
				if(abc[i][j]>max)
				{
					max = abc[i][j];
					
				}
			}
			
		}
		System.out.println(max);
	}

}