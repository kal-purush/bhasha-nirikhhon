using System;
using System.Collections.Generic;

namespace Implementations
    {
    public class AppendAndDelete
        {
        public static void Main (string[] args)
            {
            string initialString = Console.ReadLine();
            string desireString = Console.ReadLine();
            int desireOperation = Convert.ToInt32(Console.ReadLine());
            int count = 0;
            Stack<char> secondHalf = new Stack<char>();

            for ( int i = 0; i < initialString.Length; i++ )
                {
                if ( initialString[i] != desireString[i] )
                    break;
                count++;
                }

            for ( int i = count; i < initialString.Length; i++ )
                {
                secondHalf.Push(initialString[i]);
                }

            double popCount = Math.Ceiling((desireOperation / 2.0));
            double pushCount = desireOperation - popCount;
            string endString = desireString.Substring(count);

            //char[] result = new char[temp];
            bool flag = true;

            for ( int i = 0; i < popCount; i++ )
                {
                if ( secondHalf.Count == 0 )
                    continue;
                secondHalf.Pop();
                }
            if ( string.IsNullOrEmpty(endString) )
                {
                for ( int i = 0; i < pushCount; i++ )
                    {
                    secondHalf.Push(desireString[i]);
                    }
                int temp = secondHalf.Count;
                for ( int i = temp - 1; i >= 0; i-- )
                    {
                    if ( desireString[i] != secondHalf.Pop() )
                        {
                        flag = false;
                        break;
                        }
                    }
                }
            else
                {
                for ( int i = 0; i < pushCount; i++ )
                    {
                    secondHalf.Push(endString[i]);
                    }
                int temp = secondHalf.Count;
                for ( int i = temp - 1; i >= 0; i-- )
                    {
                    if ( endString[i] != secondHalf.Pop() )
                        {
                        flag = false;
                        break;
                        }
                    }
                }
            Console.WriteLine(flag ? "Yes" : "No");
            
            //Console.WriteLine("Initial String" + secondHalf.ToArray().ToString());
            //Console.WriteLine(desireString.Substring(count));
            //Console.WriteLine(popCount);
            //Console.WriteLine(pushCount);
            Console.ReadKey();
            }
        }
    }