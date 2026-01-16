using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SFML.Graphics;
using SFML.Window;

namespace Presentation
{
    class MyProgram : SFMLProgram
    {
        RenderWindow window;
        bool done;

        public MyProgram()
        {
            Initialize();
        }

        public void Initialize()
        {
            window = new RenderWindow(new VideoMode(500, 600), "Okno", Styles.Close, new ContextSettings() { DepthBits = 24, AntialiasingLevel = 4 });

            window.Closed += OnClosed;
        }

        private void OnClosed(object sender, EventArgs e)
        {

        }
    }
}