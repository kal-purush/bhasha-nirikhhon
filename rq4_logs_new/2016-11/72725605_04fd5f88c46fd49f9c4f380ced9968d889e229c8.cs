using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using SFML;
using SFML.System;
using SFML.Window;
using SFML.Graphics;
using SFML.Audio;

namespace SFML_Prog2_Gruppe1
{
    public class Game
    {
        World world;

        public Game()
        {
            world = new World();
        }

        public void Update()
        {
            //Input
            //Update
        }

        public void Draw()
        {
            world.Draw();
        }

    }
}