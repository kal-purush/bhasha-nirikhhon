using System;

namespace Pascals_triangle
{
    class Program
    {
        static void Main(string[] args)
        {
            int rows = 13; // Number of rows in the triangle (for proper formatting do not exceed 13)
            int row; // Allocating row variable (this is done here to avoid that it is unallocated when exiting the loop

            int[][] triangle = new int[rows][]; // Defining the triangle variable as an array of undefined arrays

            triangle[0] = new int[1]; // initializing the first element of the triangle
            triangle[0][0] = 1;

            /* Main calculation loop */
            for (row = 0; row < rows - 1; row++) // Looping through every row
            {
                triangle[row + 1] = new int[row + 2]; // Allocating the inner array in the current row
                Console.Write("".PadLeft((rows - row) * 2)); // Padding the current console line to create prettier formatted output

                for (int col = 0; col <= row; col++) // Looping through every collumn in the current row
                {
                    Console.Write("{0,3} ", triangle[row][col]); // printing the content of the current element
                    triangle[row + 1][col] += triangle[row][col]; // adding the value of the current element to the below row at the same collumn location
                    triangle[row + 1][col + 1] += triangle[row][col]; // adding the value of the current element to the below row at the next collumn location
                }
                Console.Write('\n'); // printing a newline character
            }

            /* Printing the last row
            ** This is done seperately to avoid adding more rows to the triangle
            */
            Console.Write("".PadLeft((rows - row) * 2)); // adding spaces
            for (int col = 0; col <= row; col++) // Looping through every collumn in the last row 
            {
                Console.Write("{0,3} ", triangle[row][col]); // printing the content of the current element
            }
            Console.Write('\n'); // printing a newline character


            Console.Read(); // wait for user input to halt program
        }
    }
}