using SFML.Graphics;
using SFML.Window;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProjectSheep
{
    class Schafgenerator
    {
        public static Schaf[] schafArray;
        public Schafgenerator(int level)
        {
            schafArray = new Schaf[20];
            for (int i = 0; i < schafArray.Length-1; i += 2)
            {
                schafArray[i] = new Schaf(new Vector2f(20, 900));
                schafArray[i + 1] = new Schaf(new Vector2f(900, 900));
            }
        }
        public void Update(GameTime gTime)
        {
            foreach (Schaf s in schafArray)
            {
                s.Update(gTime);
            }
        }
        public void Draw(RenderWindow win)
        {
            foreach (Schaf s in schafArray)
            {
                s.Draw(win);
            }
        }
    }
}