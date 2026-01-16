
public class SudokuSolver {
								 //|0 1 2|3 4 5|6 7 8|
	public static int[][] grid = { {1,5,0,2,0,0,0,0,3},
				                   {0,0,6,0,0,0,4,9,0},
				                   {3,0,0,0,0,0,0,0,0},
				                   {9,0,0,0,7,4,0,0,5},
				                   {0,0,0,8,0,5,0,0,0},
				                   {4,0,0,6,2,0,0,0,7},
				                   {0,0,0,0,0,0,0,0,4},
				                   {0,1,4,0,0,0,5,0,0},
				                   {7,0,0,0,0,2,0,6,1}};
	
	public static void boardPrinter()
	{
		for(int i = 0; i<grid.length; i++)
		{
			if(i == 0 || i == 3 || i == 6)
				System.out.print("+ - - - + - - - + - - - +\n");
			for(int j = 0; j<grid.length; j++)
			{
				if(j == 0)
					System.out.print("| "+grid[i][j]);
				else if(j==2 || j == 5)
					System.out.print(" "+grid[i][j]+" |");
				else if(j == 8)
					System.out.print(" "+grid[i][j]+" |\n");
				else
					System.out.print(" "+grid[i][j]);
				
			}
		}
		System.out.print("+ - - - + - - - + - - - +\n");
	}
	
	public static void solver()
	{
		for(int y = 0; y<grid.length; y++)
		{
			for(int x = 0; x<grid.length; x++)
			{
				if(grid[y][x] == 0)
				{
					for(int solution = 1; solution<10; solution++)
					{
						if(isPossible(y,x,solution))
						{
							grid[y][x] = solution;
							solver();
							grid[y][x] = 0;
							
						}
					}
					return;
				}
			}
		}
		boardPrinter();
	}
	
	public static boolean isPossible(int y,int x, int solution)
	{
		for(int i = 0; i <grid.length; i++)
			if(solution == grid[y][i]) return false;
		for(int i = 0; i<grid.length; i++)
			if(solution == grid[i][x]) return false;
		//determine square to check
		int square_x = -1;
		int square_y = -1;
		if(x<3) square_x = 0;
		else if(x<6) square_x = 3;
		else square_x = 6;
		if(y<3) square_y = 0;
		else if(y<6) square_y = 3;
		else square_y = 6;
		int temp_x = square_x;
		int temp_y = square_y;
		while(square_y<temp_y+3)
		{
			while(square_x<temp_x+3)
			{
				if(solution == grid[square_y][square_x]) return false;
				square_x++;
			}
			square_x = temp_x;
			square_y++;
		}
		return true;
	}
	
	
	public static void main(String[] args) {
		solver();
		

	}

}