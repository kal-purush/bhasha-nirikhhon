using System;

namespace McGee_III
{
	public class Floor
	{
		Random rnd = new Random ();

		public Tile[,] Room;
		public int Level;
		public int Size;

		public void printTiles ()
		{
			for (int y = 0; y < Size; y++) {
				for (int x = 0; x < Size; x++) {
					Console.WriteLine (Room [x, y].Descripion);
				}
			}
		}

		public Floor (int level)
		{
			Level = level;
			Size = Level * 2 + 1;
			Room = new Tile[Size,Size];
			for (int y = 0; y < Size; y++) {
				for (int x = 0; x < Size; x++) {
					Room [x, y] = new Tile (rnd.Next(1000), x, y);
				}
			}
		}
	}
}