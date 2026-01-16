/*
  File: WordSearch.java

  Description: PROGRAMMING ASSIGNMENT #11 ***WITH THE EXTRA CREDIT DIAGONALS***

  Student Name: ZACHARY GOODMAN

  Student UT EID: ZJG95

  Course Name: CS 312 

  Unique Number: 53240

  Date Created: 4/8/13

  Date Last Modified: 4/12/13

*/

//***THE EXTRA CREDIT DIAGONALS ARE IN THIS PROGRAM***

import java.io.*;
import java.util.*;

public class WordSearch
{
	public static void main(String[] args) throws IOException
	{
		File hidden = new File("hidden.txt");
		Scanner sc = new Scanner(hidden);
		File outFile = new File ("found.txt");
		FileWriter fwriter = new FileWriter(outFile);
		PrintWriter pwriter = new PrintWriter(fwriter);
		int height, length;
		height = sc.nextInt();
		length = sc.nextInt();
		String[][] a = new String[length][height];
		//System.out.printf("a.length = %d\na[0].length= %d\n", a.length,a[0].length);//UNUSED CODE
		//System.out.printf("length = %d\nheight = %d\n", length, height);//UNUSED CODE
		int countx = 0;
		int county = 0;
		while (sc.hasNext())  // build the word search
		{
			a[countx][county] = sc.next();
			countx++;
			if (countx>= length)
			{	
				countx = 0;
				county++;
			}
			if (county>=height)
				break;
		}
		int wnumber = sc.nextInt();
		String[] wordBank = new String[wnumber];
		//printArray(a);//UNUSED CODE
		String dummy = sc.nextLine();
		for (int i = 0; i<wnumber; i++)  // build the array of wordBank
		{
			wordBank[i] = sc.nextLine();
		}
		sc.close();


		for (int i = 0; i<wnumber; i++)  // pick the first word to look for
		{
			boolean located = false;  // SET WORD TO NOT FOUND
			String word = wordBank[i]; // create string from first word
			for (int y = 0; y<a[0].length; y++)  // traverse the word search
			{
				//SEARCH RIGHT

				for (int x = 0; x+word.length()<=a.length; x++)  // search right
				{
					String found = "";
					int xcopy = x;
					for (int c = 0; c < word.length(); c++)
					{
						found = found + a[xcopy][y];
						xcopy++;
					}
					if (word.equals(found))
					{
						//System.out.printf("%-11s %3d %3d\n",word, y+1, x+1);//UNUSED CODE
						pwriter.printf("%-11s %3d %3d\n",word, y+1, x+1);
						located = true;
					}
				}

				//SEARCH LEFT

				for (int x = a.length-1; x+1-word.length()>=0; x--)  // search left
				{
					String found = "";
					int xcopy = x;
					for (int c = 0; c < word.length(); c++)
					{
						found = found + a[xcopy][y];
						xcopy--;
					}
					if (word.equals(found))
					{
						//System.out.printf("%-11s %3d %3d\n",word, y+1, x+1);//UNUSED CODE
						pwriter.printf("%-11s %3d %3d\n",word, y+1, x+1);
						located = true;
					}
				}

				//SEARCH DOWN

				for (int x = 0; x<a.length; x++)  // search down
				{
					String found = "";
					int ycopy = y;
					if (ycopy>=a[0].length)
						break;
					for (int c = 0; c < word.length(); c++)
					{
						found = found + a[x][ycopy];
						ycopy++;
						if (ycopy>=a[0].length)
							break;
					}
					if (word.equals(found))
					{
						//System.out.printf("%-11s %3d %3d\n",word, y+1, x+1);//UNUSED CODE
						pwriter.printf("%-11s %3d %3d\n",word, y+1, x+1);
						located = true;
					}
				}
			}

			//SEARCH UP

			for (int y = a[0].length-1; y+1-word.length()>=0; y--)
			{
				for (int x = 0; x<a.length; x++)  // search up
				{
					String found = "";
					int ycopy = y;
					for (int c = 0; c < word.length(); c++)
					{
						found = found + a[x][ycopy];
						ycopy--;
					}
					if (word.equals(found))
					{
						//System.out.printf("%-11s %3d %3d\n",word, y+1, x+1);//UNUSED CODE
						pwriter.printf("%-11s %3d %3d\n",word, y+1, x+1);
						located = true;
					}
				}	
			}

			//SEARCH DIAGONAL - DOWN RIGHT
			
			for (int y = 0; y+word.length()<=a[0].length; y++)
			{
				for (int x = a.length-word.length(); x>=0; x--) //search diagonal DOWN RIGHT
				{
					String found = "";
					int xcopy = x;
					int ycopy = y;
					for (int c = 0; c < word.length(); c++)
					{
						found = found + a[xcopy][ycopy];
						xcopy++;
						ycopy++;
					}
					if (word.equals(found))
					{
						//System.out.printf("%-11s %3d %3d\n",word, y+1, x+1);//UNUSED CODE
						pwriter.printf("%-11s %3d %3d\n",word, y+1, x+1);
						located = true;
					}					
				}
			}

			//SEARCH DIAGONAL - DOWN LEFT
			
			for (int y = 0; y+word.length()<=a[0].length; y++)
			{
				for (int x = a.length-1; x+1-word.length()>=0; x--) //search diagonal DOWN LEFT
				{
					String found = "";
					int xcopy = x;
					int ycopy = y;
					for (int c = 0; c < word.length(); c++)
					{
						found = found + a[xcopy][ycopy];
						xcopy--;
						ycopy++;
					}
					if (word.equals(found))
					{
						//System.out.printf("%-11s %3d %3d\n",word, y+1, x+1);//UNUSED CODE
						pwriter.printf("%-11s %3d %3d\n",word, y+1, x+1);
						located = true;
					}					
				}
			}

			//SEARCH DIAGONAL - UP RIGHT
			
			for (int y = a[0].length-1; y+1-word.length()>=0; y--)
			{
				for (int x = 0; x+word.length()<=a.length; x++) //search diagonally UP RIGHT
				{
					String found = "";
					int xcopy = x;
					int ycopy = y;
					for (int c = 0; c < word.length(); c++)
					{
						found = found + a[xcopy][ycopy];
						xcopy++;
						ycopy--;
					}
					if (word.equals(found))
					{
						//System.out.printf("%-11s %3d %3d\n",word, y+1, x+1);//UNUSED CODE
						pwriter.printf("%-11s %3d %3d\n",word, y+1, x+1);
						located = true;
					}					
				}
			}

			//SEARCH DIAGONAL - UP LEFT

			for (int y = a[0].length-1; y+1-word.length()>=0; y--)
			{
				for (int x = a.length-1; x+1-word.length()>=0; x--) //search diagonally UP LEFT
				{
					String found = "";
					int xcopy = x;
					int ycopy = y;
					for (int c = 0; c < word.length(); c++)
					{
						found = found + a[xcopy][ycopy];
						xcopy--;
						ycopy--;
					}
					if (word.equals(found))
					{
						//System.out.printf("%-11s %3d %3d\n",word, y+1, x+1);//UNUSED CODE
						pwriter.printf("%-11s %3d %3d\n",word, y+1, x+1);
						located = true;
					}					
				}
			}
			if (located == false)  // CHECK IF WORD HAS BEEN FOUND
			{
				//System.out.printf("%-11s %3d %3d\n",word, 0, 0);//UNUSED CODE
				pwriter.printf("%-11s %3d %3d\n",word, 0, 0);				
			}
		}//end of final loop
		pwriter.close();
	}

	//UNUSED CODE
	/*public static void printArray(String[][] a)
	{
		for (int y = 0; y<a[0].length; y++) //PRINT THE ARRAY
		{
			for (int x = 0; x<a.length; x++)
			{
				System.out.print(a[x][y] + " ");
			}
			System.out.println();
		}
	}*/
}  // THIS ASSIGNMENT WAS A DOOZY. THANKS FOR YOUR TIME GRADING IT!