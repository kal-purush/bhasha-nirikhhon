using System;
using System.Collections.Generic;
namespace Linear_DataStructures
{
    public class StringCombination
    {

        public static void DriverCode()
        {
            StringCombination sc = new StringCombination();
            //Combination of string
           // sc.StringPermutn("abc");
            //repeating numbers
            int[] a = { 1, 2, 3, 2 };
           // sc.printRepeating(a, 4);
            //decode string
            //var sc1 = sc.decode1("2[a3[b]]");
            //array pairs
            int[] b =  {10, 12, 10, 15, -1, 7, 6, 5, 4, 2, 1, 1, 1};//{ 1, 5, 7, -1, 5 };
            sc.DisplayPairs2(b,11);

        }

        public void DisplayPairs2(int[] arr,int sum)
        {
            // Store counts of all elements in map m
            Dictionary<int,
                       int> m = new Dictionary<int,
                                               int>();

            // Traverse through all elements
            for (int i = 0; i < arr.Length; i++)
            {

                // Search if a pair can be formed with
                // arr[i].
                int rem = sum - arr[i];

                if (m.ContainsKey(rem))
                {
                    int count = m[rem];

                    for (int j = 0; j < count; j++)
                    {
                        Console.Write("(" + rem + ", " +arr[i] + ")" + "\n");
                    }
                }

                if (m.ContainsKey(arr[i]))
                {
                    m[arr[i]]++;
                }
                else
                {
                    m[arr[i]] = 1;
                }
            }
        }

        public void StringPermutn(string input)
        {
            for (int i = 0; i < input.Length; i++)
            {
                for (int j = 1; j <= input.Length - i; j++)//trick part
                {
                    var result = input.Substring(i, j);
                    Console.WriteLine(result);
                }
            }
        }

        public void printRepeating(int[] arr, int n)
        {
            // First check all the values that are
            // present in an array then go to that
            // values as indexes and increment by
            // the size of array
            for (int i = 0; i < n; i++)
            {
                int index = arr[i] % n;
                //Console.WriteLine("index="+index.ToString()+"  arr[i]"+ arr[i].ToString());
                arr[index] += n;
            }
            foreach (var i in arr)
            {
                Console.WriteLine(i.ToString());
            }

            // Now check which value exists more
            // than once by dividing with the size
            // of array
            for (int i = 0; i < n; i++)
            {
                int t = (arr[i] / n);
                //Console.WriteLine(t.ToString());
                if ((arr[i] / n) >= 2)
                {
                    Console.Write(i + " ");
                }
            }
        }
        public string decode1(string str)
        {
            Stack<int> integerstack = new Stack<int>();
            Stack<char> stringstack = new Stack<char>();
            string temp = "", result = "";

            // Traversing the string  
            for (int i = 0; i < str.Length; i++)
            {
                int count = 0;

                // If number, convert it into number  
                // and push it into integerstack.  
                if (char.IsDigit(str[i]))
                {
                    while (char.IsDigit(str[i]))
                    {
                        count = count * 10 + str[i] - '0';
                        i++;
                    }

                    i--;
                    integerstack.Push(count);
                }

                // If closing bracket ']', pop elemment  
                // until '[' opening bracket is not found  
                // in the character stack.  
                else if (str[i] == ']')
                {
                    temp = "";
                    count = 0;

                    if (integerstack.Count > 0)
                    {
                        count = integerstack.Peek();
                        integerstack.Pop();
                    }

                    while (stringstack.Count > 0 &&
                           stringstack.Peek() != '[')
                    {
                        temp = stringstack.Peek() + temp;
                        stringstack.Pop();
                    }

                    if (stringstack.Count > 0 &&
                        stringstack.Peek() == '[')
                    {
                        stringstack.Pop();
                    }

                    // Repeating the popped string 'temo'  
                    // count number of times.  
                    for (int j = 0; j < count; j++)
                    {
                        result = result + temp;
                    }

                    // Push it in the character stack.  
                    for (int j = 0; j < result.Length; j++)
                    {
                        stringstack.Push(result[j]);
                    }

                    result = "";
                }

                // If '[' opening bracket, push it  
                // into character stack.  
                else if (str[i] == '[')
                {
                    if (char.IsDigit(str[i - 1]))
                    {
                        stringstack.Push(str[i]);
                    }

                    else
                    {
                        stringstack.Push(str[i]);
                        integerstack.Push(1);
                    }
                }

                else
                {
                    stringstack.Push(str[i]);
                }
            }

            // Pop all the elmenet, make a  
            // string and return.  
            while (stringstack.Count > 0)
            {
                result = stringstack.Peek() + result;
                stringstack.Pop();
            }

            return result;
        }
        // Function to make the string unique 
        //Using Index Of
        public static String unique(String s)
        {
            String str = "";
            int len = s.Length;

            // loop to traverse the string and 
            // check for repeating chars using 
            // IndexOf() method in Java 
            for (int i = 0; i < len; i++)
            {
                // character at i'th index of s 
                char c = s[i];

                // if c is present in str, it returns 
                // the index of c, else it returns -1 
                if (str.IndexOf(c) < 0)
                {
                    // adding c to str if -1 is returned 
                    str += c;
                }
            }

            return str;
        }
        public static string Decode(string input)
        {
            string result = null;
            System.Collections.Generic.Stack<int> integerstack = new System.Collections.Generic.Stack<int>();
            System.Collections.Generic.Stack<char> charstack = new System.Collections.Generic.Stack<char>();

            for (int i = 0; i < input.Length; i++)
            {
                if (char.IsDigit(input[i]))
                {
                    integerstack.Push(input[i]);
                }
                else if (input[i] == '[')
                {
                    charstack.Push('[');
                }
                else if (input[i] == ']')
                {
                    //charstack.pop();



                }

                return result;
            }
            return null;


        }

    }
}