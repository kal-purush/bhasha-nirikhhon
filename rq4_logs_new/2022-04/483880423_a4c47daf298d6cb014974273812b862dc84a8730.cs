//Opposites Attract
public class LoveDetector
{
    public static bool lovefunc(int flower1, int flower2)
    { 
      if ((flower1 + flower2)%2 != 0)
          {return true;}
      return false;
    }
}

//How good are you really?
public static bool BetterThanAverage(int[] ClassPoints, int YourPoints)
  {
    float Sum = 0, Everage;
    for (int i = 0; i < ClassPoints.Length; i++)
    {
      Sum += ClassPoints[i];
    }
    Everage = Sum/ClassPoints.Length;
    
    if (YourPoints > Everage)
      return true;
    return false;
  }

  //Opposite number
  public static int Opposite(int number)
        {
              return 0 - number;
        }

//Complete the method that takes a boolean value and return a "Yes" string for true, or a "No" string for false.

public static string boolToWord(bool word)
  {
    //TODO
    string yes="Yes";
    string no = "No";
    if(word == true)
      return yes;
    return no;
  }

//In this simple assignment you are given a number and have to make it negative. But maybe the number is already negative?
 public static int MakeNegative(int number)
  {
    // Code?
    if(number < 0)
      return number;
    if(number > 0)
      return number*-1;
    return 0;
  }

//Given an array of integers your solution should find the smallest integer.
public static int FindSmallestInt(int[] args) 
    {
      int min = args[0];
        for(int i=0; i < args.Length; i++)
          if(args[i] < min)
            min = args[i];
      return min;
    }

//Write a program that finds the summation of every number from 1 to num. The number will always be a positive integer greater than 0.

public static int summation(int num)
    {
      int sum=0;
      for (int i=1;i<=num;i++)
        sum=sum+i;
      return sum;
    }

//You get an array of numbers, return the sum of all of the positives ones.
//Note: if there is nothing to sum, the sum is default to 0.

public static int PositiveSum(int[] arr)
  {
    int sum=0;
    for(int i=0;i<arr.Length;i++)
      if(arr[i] > 0)
        sum=sum+arr[i];
    return sum;
  }

//Create a function which answers the question "Are you playing banjo?".
//If your name starts with the letter "R" or lower case "r", you are playing banjo!

//The function takes a name as its only argument, and returns one of the following strings:

//name + " plays banjo"
//name + " does not play banjo"
//Names given are always valid strings.

public static string AreYouPlayingBanjo(string name)
{
    //Implement me
    if (name.StartsWith("R") || name.StartsWith("r"))
    {
        return (name + " plays banjo");
    }
    else return (name + " does not play banjo");
}

//Given a non-empty array of integers, return the result of multiplying the values together in order. Example:

//[1, 2, 3, 4] => 1 * 2 * 3 * 4 = 24

public static int Grow(int[] x)
{
    var sum = 1;
    for (var i = 0; i < x.Length; i++)
    {
        sum *= x[i];
    }
    return sum;
}

//If you can't sleep, just count sheep!!

//Task:
//Given a non-negative integer, 3 for example, return a string with a murmur: "1 sheep...2 sheep...3 sheep...".Input will always be valid, i.e. no negative integers.

public static string CountSheep(int n)
{

    string a = "";
    if (n >= 1)
        for (int i = 1; i <= n; i++)
        {
            a += (i + " sheep...");
        }

    return a;
    throw new NotImplementedException();
}
//Create a function with two arguments that will return an array of the first (n) multiples of(x).

//Assume both the given number and the number of times to count will be positive numbers greater than 0.

//Return the results as an array (or list in Python, Haskell or Elixir).

//Examples:

//countBy(1, 10)  should return  { 1,2,3,4,5,6,7,8,9,10}
//countBy(2, 5)  should return { 2,4,6,8,10}
public static int[] CountBy(int x, int n)
{
    int[] z = new int[n];

    for (int i = 1; i <= n; i++)
    {
        z[i - 1] = x * i;
    }

    return z;
}

//Clock shows h hours, m minutes and s seconds after midnight.

//Your task is to write a function which returns the time since midnight in milliseconds.

//Example:
//h = 0
//m = 1
//s = 1

//result = 61000
public static int Past(int h, int m, int s)
{
    //#Happy Coding! ^_^
    int num = ((h * 3600) + (m * 60) + s) * 10 * 10 * 10;
    return num;
}

//Sum Arrays
using System;

public class Kata
{
  public static double SumArray(double[] array)
  {
    double result = 0;
    for (int i = 0; i < array.Length;++i) {
      result += array[i];
    }
    return result;
  }
}

//Century From Year
using System;
public static class Kata
{
  public static int СenturyFromYear(int year)
  {
    if (year % 100 == 0) return (int)year/100;
    else return (int)year/100 + 1;
  }
}

//Convert a Number to a String!
using System;

public class Kata
{
  public static string NumberToString(int num)
  {
    string s = num.ToString();
    return s;
  }
}

//Abbreviate a Two Word Name
using System;
public class Kata
{
  public static string AbbrevName(string name)
  {
    string s = name[0] + ".";
    
    for (int i = 1; i < name.Length; ++i) {
      if (name[i] == ' ') s = s + name[i+1];
    }
    s = s.ToUpper();
    return s;
  }
}