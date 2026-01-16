using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MinMax {
    class Program {
        static void Main(string[] args) {
            Random random = new Random();

            int[] arr = new int[1000];

            //random elemek hozzaadasa a tombhoz
            for (int i = 0; i < arr.Length; i++) {
                arr[i] = random.Next(Int32.MinValue,Int32.MaxValue);
            }


            //tomb elemeinek kiiratasa
            /*for (int i = 0; i < arr.Length; i++) {      
                Console.WriteLine(arr[i]);
            }*/

            int min = Int32.MaxValue;
            int min_i = -1;

            for (int i = 0; i < arr.Length; i++) {
                if(min > arr[i]) {
                    min = arr[i];
                    min_i = i;
                }
            }



            Console.ReadKey();
        }
    }
}