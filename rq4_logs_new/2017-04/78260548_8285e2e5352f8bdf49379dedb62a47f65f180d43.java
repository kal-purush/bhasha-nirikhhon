package com.tutorial.pattern;

public class HollowSquare {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		draw(5);
	}
	
	public static void draw(int totalHeight){
		for(int i=0; i<totalHeight; i++)
		{
			
			for(int j=0; j<totalHeight; j++)
			{
				if(i>0 && i<totalHeight-1)
				{
					if(j>0 && j<totalHeight-1)
					{
						System.out.print(" ");
					}
					else
					{
						System.out.print("*");
					}
				}
				else
				System.out.print("*");
			}
			System.out.println();
		}
	}
}