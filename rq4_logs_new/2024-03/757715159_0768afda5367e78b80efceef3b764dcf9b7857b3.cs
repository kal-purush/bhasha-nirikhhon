using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CMP1903_A1_2324
{
    internal class Die
    {
        private static Random random = new Random(); //Creating the random instance

        public int Roll() //Roll constructor
        {
            return random.Next(1,7); //Assigning all dice methods their value
        }
    }
}