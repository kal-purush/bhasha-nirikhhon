using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Game
{
    class Entities
    {
        private int x = 0;
        private int y = 0;
        private char letter = ' ';

        public int X { get { return x; } set { x = value; } }
        public int Y { get { return y; } set { y = value; } }
        public char Letter { get { return letter; } set { letter = value; } }

        public void UpdateBoard(char[,] array)
        {
            
        }
    }
}