using System;
using Program.cs;

public class shipSpawn
{
	public shipSpawn()
	{
        Random rnd = new Random();
        bool VerticalOrH;
        int position;
        int shipsX, shipsY;

        void drawShip(int shipLen, int direction, int position)
        {
            for (int i = 0; i < shipLen; i++)
            {
               
            }

        }

        bool trueOrFalse()
        {
            if (rnd.Next(1) > 0)
            {
                VerticalOrH = true;
            }
            else
            {
                VerticalOrH = false;
            }
        }

        void destroyer(int x,int y,int maxX, int maxY)
        {
            trueOrFalse();

            if (VerticalOrH) //if vertical
            {
                shipsY = rnd.Next(0, maxY - 1);



            }
            else            // if horizontal
            {
                shipsX = rnd.Next(0, maxX - 1);
            }

        }



    }
}