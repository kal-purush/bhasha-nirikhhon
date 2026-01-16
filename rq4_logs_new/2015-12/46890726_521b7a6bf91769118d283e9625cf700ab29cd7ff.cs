using System;

namespace McGee_III
{
    class Tile
    {
		enum Types { Start, Empty, Key, Door}

        // Properties
        public string Descripion;
        public int Xpos;
        public int Ypos;
		public int Type;
        public int Seed;

		public void sense()
		{
			if (Type == (int)Types.Door) {
				Console.WriteLine ("\nYou feel trap door beneath your feet.");
			}
		}

        // Constructor
        public Tile(int seed, int x, int y)
        {
            Xpos = x;
            Ypos = y;
            Seed = seed;
			Descripion = String.Format("Tile:\t({0},{1})\tType:\t{2}\tSeed:\t{3}", Xpos, Ypos, Type, Seed);
        }
    }
}