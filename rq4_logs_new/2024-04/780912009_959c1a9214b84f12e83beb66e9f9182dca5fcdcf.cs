using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Connect4
{
    internal class bot 
    {
        private Random rnd = new Random();
    

        public bot()
        {
        }

        

        public bot(Random rnd )
        {
            this.rnd = rnd;
           
        }

        public int FindMove()
        {
            int randomColumn = rnd.Next(0, 7); // Adjust range according to the board width
            return randomColumn;
        }

        
    }
}