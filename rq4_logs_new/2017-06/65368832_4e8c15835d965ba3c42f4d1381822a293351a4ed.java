package Hunter;

import java.util.*;

public class Closest_Zero {

	public static void main(String[] args) 
	{
		int size,i;
		Scanner sc = new Scanner(System.in);
		System.out.println("Enter the size: ");
		size = sc.nextInt();
		float[] num = new float[size];
		for(i=0;i<size;i++)
			num[i] = sc.nextFloat();
			
		for(i=0;i<size-1;i++)
		{
			if(((num[i]+num[i+1])>-1)&&((num[i]+num[i+1])<1))
				System.out.println("closest to zero Numbers are: "+num[i]+"    "+num[i+1]);
		}

	}

}