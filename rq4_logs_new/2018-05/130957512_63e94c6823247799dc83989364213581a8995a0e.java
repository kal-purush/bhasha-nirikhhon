package src;

public class ArrayRotate {
	
	public int[] rotate(int[] arr, int rcount)
	{
		int size = arr.length;
		int[] temp = new int[arr.length];
		if(rcount<size)
		{
			for(int i=0; i<rcount; i++)
			{
				temp[i] = arr[i];
			}
			for(int l=0; l<rcount; l++)
			{
				for(int j=0;j<size-1;j++)
				{
					arr[j] = arr[j+1];
				}
			}
			System.arraycopy(temp, 0, arr, size-rcount, rcount);		
			
		}
		return arr;
	}
	
}