using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HuntTheWumpus
{
    class Cave
    {
        //instance variables
        private Room[,] myCave = new Room[5, 6];

        //constructors
        public Cave(String File)
        {
            for(int i = 0; i < 5; i++)
            {
                for(int j = 0; j < 6; j++)
                {
                    myCave[i, j] = new Room();
                }
            }
            //generate a cave from a file
            //fill Cave
        }
        //Probably gonna delete this constructor if I cant end up figuring it out. 
        public Cave()
        {
            for (int i = 0; i < 5; i++)
            {
                for (int j = 0; j < 6; j++)
                {
                    myCave[i, j] = new Room();
                }
            }
            Console.WriteLine("asdhfffhasdfhasdfhasdhfasdhfasdhf");
            //randomly generate a cave
        }
        //mutators
            //cave will remain static throughout. 
        //accesors
        public int[] adjacentRooms()
        {
            int[] adjacentRooms = { 1, 2, 3, 4, 5, 6 }; // PLACEHOLDER 
            //top left, top, top right, bottom left, bottom, bottom right
            //    __
            //   /  \
            //   \__/ 
            return adjacentRooms;
        }
        public int[] accessibleRooms()
        {
            int[] accessibleRooms = { 0, 0, 3, 2, 0, 1 };
            //top left, top, top right, bottom left, bottom, bottom right
            //    __
            //   /  \
            //   \__/ 
            // 0 will be closed room
            // other values will indicate what room openings are available 
            return accessibleRooms; 
        }
    }
}