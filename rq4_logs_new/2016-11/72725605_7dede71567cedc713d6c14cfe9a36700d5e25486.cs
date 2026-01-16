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
    public class ProjectRenderWindow
    {
        static RenderWindow window = null;

        private ProjectRenderWindow()
        {
        }

        public static RenderWindow GetRenderWindowInstance()
        {
            if(window == null)
            {
                window = new RenderWindow(new VideoMode(1280, 720), "Prog2_Gruppe1", Styles.Close, new ContextSettings(24, 8, 2));
                return window;
            }
            return window;
        }
    }
}