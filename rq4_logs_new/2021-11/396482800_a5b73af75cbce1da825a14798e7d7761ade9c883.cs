using System;
using System.Collections.Generic;
using System.Text;
using static System.Console;

using HomeworkMargaret.Tools;

namespace HomeworkMargaret.General
{
    class Homework5_DoubleArrays
    {
        public static void Start()
        {
            CT.Print("Enter height: ", false);
            int height = Convert.ToInt32(ReadLine());
            CT.Print("Enter width: ", false);
            int width = Convert.ToInt32(ReadLine());

            int[,] array = new int[height, width];

            int usersChoice;
            do
            {
                CT.Space();
                usersChoice = CT.Menu(
                    "manual array initialization",
                    "random numbers array",
                    "show array",
                    "change an element in array",
                    "count and show amount of even and odd numbers",
                    "sum of elements in particular line",
                    "sum of elements in particular column",
                    "check value",
                    "sort values vertically",
                    "sort values horizonally");
                CT.Space();

                switch(usersChoice)
                {
                    case 1:
                        ManualArray(array);
                        break;

                    case 2:
                        RandomNumbers(array);
                        break;

                    case 3:
                        ShowArray(array);
                        break;

                    case 4:
                        ChangeElement(array);
                        break;

                    case 5:
                        CountEvenOdd(array);
                        break;

                    case 6:
                        int lineNum = CT.EnterInt("enter the line: ");
                        int result = SumElementsInLine(array, lineNum);
                        CT.Print(result, "sum of elements in line ");
                        break;

                    case 7:
                        int columnNum = CT.EnterInt("enter the column: ");
                        int res = SumElementsInColumn(array, columnNum);
                        CT.Print(res, "sum of elements in column ");
                        break;

                    case 8:
                        bool valueIsFound = CheckValue(array, out int indxI, out int indxJ);
                        if (valueIsFound)
                        {
                            CT.Print("Value is found in array.");
                            CT.Print(indxI, "indxI");
                            CT.Print(indxJ, "indxJ");
                        }
                        else
                        {
                            CT.Print("Value is not found");
                        }
                        break;

                    case 9:
                        SortVertically(array);
                        break;

                    case 10:
                        SortHorizonally(array);
                        break;

                    case 0:
                        WriteLine("next time you'll die");
                        break;

                    default:
                        CT.ErrorMsg("die, stupid");
                        break;
                }

            } while (usersChoice != 0);
        }

        public static void ManualArray(int[,] array)
        {
            int height = array.GetLength(0);
            int width = array.GetLength(1);
            
            for (int i = 0; i < height; i++)
            {
                for(int j = 0; j < width; j++)
                {
                  array[i, j] = CT.EnterInt($"Enter values [{i}, {j}]: ");
                }
            }
            CT.Print("Array is initialized. Ready to join Batman!");
        }

        public static void RandomNumbers(int[,] array)
        {
            int height = array.GetLength(0);
            int width = array.GetLength(1);

            Random rand = new Random();

            for (int i = 0; i < height; i++)
            {
                for (int j = 0; j < width; j++)
                {
                    array[i, j] = rand.Next(0, 100);
                }
            }
        }

        public static void ShowArray(int[,] array)
        {
            CT.Print("Showing array");
            for (int i = 0; i < array.GetLength(0); i++)
            {
                for (int j = 0; j < array.GetLength(1); j++)
                {
                    CT.Print($"[{i},{j}] = {array[i, j]} ", false);
                }
                CT.Print("");
            }
        }

        public static void ChangeElement(int[,] array)
        {
            ShowArray(array);

            int val1 = CT.EnterInt("enter the first number of value you wanna change: ");
            int val2 = CT.EnterInt("enter the second number of value you wanna change: ");

            int newVal = CT.EnterInt("write new value: "); 

            array[val1, val2] = newVal;            
            
            CT.Print("The value has been changes. Old value has been sent to Arkham.");            
        }

        public static void CountEvenOdd(int[,] array)
        {
            int countEven = 0;
            int countOdd = 0;
            for (int i = 0; i < array.GetLength(0); i++)
            {
                for (int j = 0; j < array.GetLength(1); j++)
                {
                    if (array[i, j] == 0)
                    {
                          continue;
                    }

                    if (array[i, j] % 2 == 0)
                    {
                        countEven++;
                    }
                    else
                    {
                        countOdd++;
                    }

                }
            }

            CT.Print(countEven, "Quantity of even numbers");
            CT.Print(countOdd, "Quantity of odd numbers");
        }

        public static int SumElementsInLine(int[,] array, int lineNum)
        {            
            int sum = 0;
            for (int indx = 0; indx < array.GetLength(1); indx++)
            {
                sum = sum + array[lineNum, indx];
            }

            return sum;
        }

        public static int SumElementsInColumn(int[,] array, int columnNum)
        {
            int sum = 0;
            for (int indx = 0; indx < array.GetLength(0); indx++)
            {
                sum = sum + array[indx, columnNum];
            }

            return sum;
        }
        public static bool CheckValue(int[,] array, out int indxI, out int indxJ)
        {
            indxI = -1;
            indxJ = -1;
            int value = CT.EnterInt("enter the value you wanna find: ");
            for (int i = 0; i < array.GetLength(0); i++)
            {
                for (int j = 0; j < array.GetLength(1); j++)
                {
                    if (array[i, j] == value)
                    {
                        indxI = i;
                        indxJ = j;

                        return true;
                    }                    
                }
            }
            return false;
        }

        public static void SortVertically(int[,] array)
        {
            for(int rowIndx = 0; rowIndx < array.GetLength(1); rowIndx++)
            {
                bool needSort = true;
                while(needSort)
                {
                    needSort = false;
                    for(int lineIndx = 0; lineIndx < array.GetLength(0) - 1; lineIndx++)
                    {
                        if(array[lineIndx, rowIndx] > array[lineIndx + 1, rowIndx])
                        {
                            int box = array[lineIndx, rowIndx];
                            array[lineIndx, rowIndx] = array[lineIndx + 1, rowIndx];
                            array[lineIndx + 1, rowIndx] = box;

                            needSort = true;
                        }
                    }
                }
            }
        }

        public static void SortHorizonally(int[,] array)
        {
            for(int lineIndx = 0; lineIndx < array.GetLength(0); lineIndx++)
            {
                bool needSort = true;
                while(needSort)
                {
                    needSort = false;
                    for(int rowIndx = 0; rowIndx < array.GetLength(1) - 1; rowIndx++)
                    {
                        if(array[lineIndx, rowIndx] > array[lineIndx, rowIndx + 1])
                        {
                            int box = array[lineIndx, rowIndx];
                            array[lineIndx, rowIndx] = array[lineIndx, rowIndx + 1];
                            array[lineIndx, rowIndx + 1] = box;

                            needSort = true;
                        }
                    }
                }
            }
        }
    }
}