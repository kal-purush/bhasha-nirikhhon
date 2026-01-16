using System;
using System.Collections.Generic;
using System.IO;

/*

	………. ,*˜*, ……. ,•°*•, Spencer MK Smith ,•*°•, …… ,*˜*, .……… 
	„*˜……“• •“………'·,¸¸,‘•°. HackerRank .°•',¸¸,·'……..“• •“……˜*„
	“‹„¸¸„ •°`'·.,¸¸,.·‘…Coin Change Problem…'·.,¸¸,.·'´°•„¸¸„›“

	This dynamic programming challenge of moderate difficulty can be found here: https://www.hackerrank.com/challenges/coin-change

	At first I came to a solution very quickly to this problem, but my solution had a flaw: If the amount we were looking for was 5,
	it would consider both coin choices of 2,3 and 3,2 as a solution, but these should only be counted as one solution.  Based on this
	flaw, I needed to rethink my whole solution into what I have now.
	
	This solution could easily be modified to use recursion, but I opted to use a bottom-up dynamic programming solution instead of
	the recursive method, either way, the logic is the same.  To begin, we are given a number of coin values and an amount.  We are to
	figure out how many different ways we can use the coin values to construct the amount.  To do this, we first figure out how many ways
	we can construct the amount = 1 only using the 1st coin given, then the amount = 2 with only the 1st coin, all of the way to the amount.
	Then, we do the same thing but also looking at the second coin.  Each of these values use each other to create themselves.  The values are
	stored in a 2D array called numberOfSolutionsAtValue with index values [number of coin values + 1, amount + 1].  Let's look at an example:
	
	Say the given coin amounts are 2,3,5,6 and the amount we want to look at is 5.  We first construct an array with index [number of coin values + 1, amount + 1]
	or, [5,6]:
	_______	Amounts														_______ Amounts
	|000000  First, we fill in the base cases.  These are that			|100000
	|000000  that is 1 way to construct the amount 0 for any			|100000
	|000000  coin amounts.  And, that given no coins and a positive -->	|100000
	|000000  amount, no amount can be constructed.  This forms the 		|100000
	|000000	 array to look like this									|100000
coinIndex															coinIndex

	Now that we have the base case values in, we start to look at the actual values, we start with the first row and go right, looking at each value.
	Looking at the amount = 1, and coinIndex = 1.  Keep in mind that coinAmount[1] = 2, because 2 is the first coin value given.  So, how many ways can
	we construct the amount = 1, using a coin with value = 2?  That isn't possible, so that value stays 0, what about amount = 2?  There would be 1 way.
	But, it gets very complex very easily as we get to higher values, and so we need to start using the values that we create earlier in the process to 
	discover the new values.  Take the amount = 5, with a coinIndex = 3, this means that at first we only look at coin value = 5.  There are a number of things we need
	to consider, first, consider that we use the coin that we're looking at with a value of 5, we can use this coin and this coin only.  Or, we could ignore the coin
	we're looking at and use another coin.  We need to take both tracks into consideration.  And so, the value at index [3][5] is the addition of these two values:
		numberOfSolutionsAtValue[3][5 - coinUsed] = numberOfSolutionsAtValue[3][0] = 1
		numberOfSolutionsAtValue[3 - 1][5] = numberOfSolutionsAtValue[2][5] (look at coinValue[2] = 3 and how many ways we can construct 5)
		
	Once we iterate through the whole 2D array, discovering every value, we simply print the value at the bottom right of the array, which is the number of ways
	that we can construct the amount given, using all of the coin values given.  After all of the iteration, the following matrix is created:
	
	_______Amount
	|100000			NOTE: There is 0 ways to construct an amount of
	|101010				  1 with any of the coins given, so those values are all 0
	|101111
	|101112
	|101112 <-- Answer = 2
coinIndex

	
	I urge you, before going further, to write out this sample problem on a piece of paper and iterate through it programmatically like your algorithm will
	be so that you can understand it better.
*/															
class Solution {

    static void Main(String[] args) {
        //Read first line of input and get the amount from it
        string[] temp_input = Console.ReadLine().Split(' ');
        int amount = Convert.ToInt32(temp_input[0]);
        
        //Read second line of input, create an array of different coin amounts and get the number of coins we have
        string[] arr_temp = Console.ReadLine().Split(' ');
        int[] coinAmountArray = Array.ConvertAll(arr_temp,Int32.Parse); 
        int numberOfCoinAmounts = coinAmountArray.Length;
        
        long[,] numberOfSolutionsAtValue = new long[numberOfCoinAmounts+1, amount + 1 ]; //NOTE: long data type needs to be used because some test cases have extremely large values
        
		//Base case, there is 1 way to construct an amount of 0 regardless of what coins we look at
        for(int n = 0; n < numberOfCoinAmounts + 1; n++)
            numberOfSolutionsAtValue[n,0] = 1;
        
		//Base case, there is 0 ways to construct a positive amount with no coins
        for(int n = 1; n < amount + 1; n++)
            numberOfSolutionsAtValue[0,n] = 0;
                        
		//Populate the rest of the values in te 2D array
		for( int currCoinIndex = 1; currCoinIndex < numberOfCoinAmounts + 1; currCoinIndex++ ) //Loop through every coins value available
        {
			for( int currAmount = 1; currAmount < amount + 1; currAmount++ ) //Loop through every amount up to the one given
            {
				int valueAfterDec = currAmount - coinAmountArray[currCoinIndex - 1]; //Value after decrementing the current coin value that we're looking at
				
				if(valueAfterDec >= 0) //If it is possible to use the coin we're looking at, this value will be greater than 0
					numberOfSolutionsAtValue[currCoinIndex,currAmount] = numberOfSolutionsAtValue[currCoinIndex, valueAfterDec] + numberOfSolutionsAtValue[currCoinIndex - 1,currAmount];
				else
					numberOfSolutionsAtValue[currCoinIndex,currAmount] = numberOfSolutionsAtValue[currCoinIndex-1,currAmount]; //If the coin value we're looking at is too large, only look at using smaller coins				
            }
        }

		//The answer resides at the last value in the 2D array
        Console.WriteLine(numberOfSolutionsAtValue[numberOfCoinAmounts, amount]);
    }
}