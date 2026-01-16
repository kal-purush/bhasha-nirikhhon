using System;
using OpenTK;
using OpenTK.Graphics;
using OpenTK.Input;

namespace OpenTKPlayground
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var window = new Window(800, 450, "Big Test"))
            {
                window.Run(60.0);
            }
        }
    }
    class Window : GameWindow
    {
        public Window(int width, int height, string title) : base(width, height, GraphicsMode.Default, title)
        {

        }
        protected override void OnUpdateFrame(FrameEventArgs e)
        {
            // Gets the KeyboardState for this frame. KeyboardState allows us to check the status of keys.
            var input = Keyboard.GetState();

            // Check if the Escape button is currently being pressed.
            if (input.IsKeyDown(Key.Escape))
            {
                // If it is, exit the window.
                Exit();
            }

            base.OnUpdateFrame(e);
        }
    }
}