package com.sudoku;

import java.util.HashSet;
import java.util.Set;

public class Sudoku
{
	private int sudokuData[][];
	private static final Set<Integer> NUMBERS= new HashSet<Integer>();
	public Sudoku()
	{
		sudokuData = new int[9][9];
		init();
	}

	public int getData(int row, int col)
	{
		validateInput(row, col, 1);
		return sudokuData[row][col];
	}
	public boolean setData(int row, int col, int val)
	{
		validateInput(row, col, val);
		if (validate(row, col, val))
		{
			sudokuData[row][col] = val;
			return true;
		} else
		{
			return false;
		}
	}

	public boolean validate(int row, int col, int val)
	{
		validateInput(row, col, val);
		if (getPossibleVals(row,col).contains(val))
		{
			return true;
		}
		else
		{
			return false;
		}
	}
	
	public Set<Integer> getPossibleVals(int row, int col)
	{
		validateInput(row, col, 1);
		Set<Integer> possibleVals= xValues(row);
		possibleVals.retainAll(yValues(col));
		possibleVals.retainAll(boxValues(row, col));
		return possibleVals;
	}
	private Set<Integer> xValues(int row)
	{
		Set<Integer> xVals= new HashSet<>(NUMBERS);
		for(int i=0;i<9;i++)
		{
			int data= sudokuData[row][i];
			if (data!=-1)
			{
			xVals.remove(data);
			}
		}
		
		return xVals;
	}
	private Set<Integer> yValues(int col)
	{
		Set<Integer> yVals= new HashSet<>(NUMBERS);
		for(int i=0;i<9;i++)
		{
			int data= sudokuData[i][col];
			if (data!=-1)
			{
			yVals.remove(data);
			}
		}
		
		return yVals;
	}
	private Set<Integer> boxValues(int row, int col)
	{
		Set<Integer> boxVals= new HashSet<>(NUMBERS);
		int boxX= row/3;
		int boxY= col/3;
		
		for(int i= boxX*3;i< ((boxX+1)*3); i++)
		{
			for(int j= boxY*3;j< ((boxY+1)*3); j++)
			{
				int data= sudokuData[i][j];
				if (data!=-1)
				{
				boxVals.remove(data);
				}
			}
		}
		
		return boxVals;
	}
	private void init()
	{
		for (int i = 0; i < 9; i++)
		{
			for (int j = 0; j < 9; j++)
			{
				sudokuData[i][j] = -1;
			}
			NUMBERS.add(i+1);
		}
		
	}
	private void validateInput(int row,int col, int val)
	{
		if (row<0||row>8||col<0||col>8||val<1||val>9)
		{
			throw new IllegalArgumentException("Wrong values");
		}
	}

}