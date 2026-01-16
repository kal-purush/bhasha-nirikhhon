using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SFML_Prog2_Gruppe1
{
    public class Room
    {
        private Tile[,] tilemap;
        private int id;

        public Tile[,] Tilemap
        {
            get { return tilemap; }
            set { tilemap = value; }
        }

        public int ID
        {
            get { return id; }
            set { id = value; }
        }


        public Room()
        {

        }

        public Room(Tile[,] tilemap, int ID)
        {
            this.tilemap = tilemap;
            this.id = ID;
        }
    }
}