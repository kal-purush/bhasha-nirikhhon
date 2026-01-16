using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpBasics
{
    class Program
    {
        static void Main(string[] args)
        {
            //SwitchLine();
            //SwapNumbers(2, 3);
        }

        private static void SwitchLine()
        {
            Console.WriteLine("Hello: \nMykhailo Salnikov");
            Console.ReadLine();
        }
        /// <summary>
        /// Swaping numbers provided in Main method
        /// </summary>
        /// <param name="x">First parameter for swaping</param>
        /// <param name="y">Second parameter for swaping</param>
        private static void SwapNumbers(int x, int y)
        {
            #region Swaping
            int temp = 0;
            temp = x;
            x = y;
            y = temp;
            #endregion

            Console.WriteLine("New value: {0}, new value {1}", x, y);
            Console.ReadLine();
        }


    }
}