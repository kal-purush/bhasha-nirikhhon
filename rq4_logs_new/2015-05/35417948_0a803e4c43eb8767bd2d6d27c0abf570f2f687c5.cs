using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MinionWorld
{
    public class Banana
    {
        Bitmap banana;

        public float X { get; set; }
        public float Y { get; set; }
        public static float Speed { get; set; }
        public Banana(float x, float y)
        {
            X = x;
            Y = y;
            banana = new Bitmap(MinionWorld.Properties.Resources.Banana);
            Speed = 5;
        }

        //metod za iscrtuvanje na Bananata
        public void Draw(Graphics g)
        {
            if (banana != null)
            {
                g.DrawImage(banana, X, Y);
            }
        }

        public void Move(float MonkeyX, float MonkeyY)
        {
            //ako pozicijata na bananata dojde vo kontakt so pozicijata na minionot
            if (Y > MonkeyY-20 && Y < MonkeyY + 90 && X > MonkeyX-20 && X < MonkeyX + 80)
            {
                
                    banana = null;

                    X = 0;
                    Y +=300; // dodaj vrednost za da bananata ne dojde na pozicija 540 so sto ke se odzeme zivot
                    Score.Value++; // dodaj poeni
                
            }

            else
            {
                Y += Speed; // brzinata na paganje na bananata
                if (Y==540) // ako pozicijata e zemjata, odzemi zivot
                {
                    banana = null;
                    Score.Lives--;
                }
            }
        }
    }
}