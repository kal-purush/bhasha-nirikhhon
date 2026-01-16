package Practice;

public class binarySearch 
	{
		private static int bS(int[] arr, int left, int right, int target)
		{
			while(right > left) 
			{
				int mid = left + (right - left) / 2;
				
				if(arr[mid] == target)
				{
					return mid;
				}
				if(arr[mid] > target)
				{
					return bS(arr, left, mid - 1, target);
				} else 
				{
					return bS(arr, mid + 1, right, target);
				}
			}
			return -1;
		}	

	public static void main(String[] args) 
	{
		// TODO Auto-generated method stub
		int arr[] = {1,2,3,4,5,6,7,8,9,10};
		
		int result = bS(arr, 0, arr.length - 1,2);
		
		if(result == -1)
			System.out.print("Can't found");
		else 
			System.out.print("Index " + result);
	}

}