package freshly;
//Towers Of Hanoi

class TowersOfHanoi
{
	
	void TOH(int n,char from_rod, char to_rod, char using_rod)
	{
		
		if(n>1)
		{
			TOH(n-1,from_rod,using_rod,to_rod);
			System.out.println("Move Disk "+n+" from rod "+from_rod+" to rod "+to_rod);
			TOH(n-1,using_rod,to_rod,from_rod);
		}
		
		else if(n==1)
		{
			System.out.println("Move Disk 1 from rod "+from_rod+" to rod "+to_rod);
			return;
		}
		
	}

  public static void main(String[]args)
  {
	  // 3 towers A, B, C and 3 disks are there
	  new TowersOfHanoi().TOH(3, 'A', 'B', 'C');
  }
}