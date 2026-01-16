using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Implementations
    {
    public class ACMICPCTeam
        {
        public static void Main (string[] args)
            {
            string[] nm = Console.ReadLine().Split(' ');
            int n = Convert.ToInt32(nm[0]);
            int m = Convert.ToInt32(nm[1]);

            int[] arr = new int[n];
            for ( int i = 0; i < n; i++ )
                {
                arr[i] = Convert.ToInt32(Console.ReadLine(), 2);
                }
            int max = 0;
            int count = 0;
            for ( int i = 0; i < n; i++ )
                {
                for ( int j = i + 1; j < n; j++ )
                    {
                    int value = arr[i] | arr[j];
                    //Convert.
                    int temp = Convert.ToString(value, 2).Replace("0","").Length;
                    if ( temp > max )
                        {
                        max = temp;
                        count = 1;
                        }
                    if ( temp == max )
                        count++;
                    }
                }
            Console.WriteLine(max);
            Console.WriteLine(count);
            Console.ReadKey();
            }
        }
    }