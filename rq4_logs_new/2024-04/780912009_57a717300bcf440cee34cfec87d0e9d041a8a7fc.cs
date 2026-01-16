using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Connect4
{
    public  class Cells
    {

        private string id;
        private string name;
        private Array Cell;
       
        public Cells()
        {
            PlayerId = -1; // Default value, indicating no player
        }

        public int PlayerId { get; internal set; }

        public void SetPlayerId(int id)
        {
            PlayerId = id;
        }
    }
}