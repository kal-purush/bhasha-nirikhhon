//		******
//		 *****
//		  ****
//		   ***
//		    **
//		     *


public class Pattern5 {

	public static void main(String[] args) {
		
		Pattern(6);
	}
	public static void Pattern(int n)
	{
		int i,j;
		for(i = 0; i < n ; i++ )
		{
			for(j = 0 ; j <  n ; j++)
			{
				if(j >= i && j < n)
				{
					System.out.print("*");
				}else{
					System.out.print(" ");
				}
			}
			System.out.print("\n");
		}
	}

}