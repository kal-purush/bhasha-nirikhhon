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
    class Program
    {
        static void Main()
        {
            RenderWindow window = new RenderWindow(new VideoMode(1280, 720), "Prog2_Gruppe1", Styles.Close, new ContextSettings(24, 8, 2));
            window.SetActive();


            while(window.IsOpen)
            {
                //Update
                //Draw
                window.Clear(Color.White);
                window.Display();
            }

        }
    }
}