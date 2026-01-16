using System;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;

namespace MononokeEngine.Graphics
{
    
    internal class ViewHandler
    {
        public int Width { get; private set; }
        public int Height { get; private set; }
        
        public int ViewWidth { get; private set; }
        
        public int ViewHeight { get; private set; }
        
        
        public Viewport Viewport { get; private set; }
        
        public Matrix ScreenMatrix { get; private set; }
        
        
        public bool Fullscreen { get; private set; }




        private readonly GraphicsDeviceManager _graphics;
        
        private bool _resizing;
        
        
        
        internal ViewHandler(int width, int height, int windowWidth, int windowHeight, bool fullscreen)
        {
            Width = width;
            Height = height;
            
            MononokeGame game = MononokeGame.Instance;
            
            _graphics = new GraphicsDeviceManager(game);
            _graphics.DeviceReset += OnGraphicsReset;
            _graphics.DeviceCreated += OnGraphicsCreate;
            
            game.Window.AllowUserResizing = true;
            game.Window.ClientSizeChanged += OnClientSizeChanged;

            Fullscreen = fullscreen;
            if (fullscreen)
            {
                SetFullscreen();
                UpdateView(
                    GraphicsAdapter.DefaultAdapter.CurrentDisplayMode.Width,
                    GraphicsAdapter.DefaultAdapter.CurrentDisplayMode.Height
                );
            }
            else
            {
                SetWindowed(windowWidth, windowHeight);
                UpdateView(windowWidth, windowHeight);
            }
        }
        
        
        public void SetFullscreen()
        {
            Mononoke.Logger.Print($"Entering fullscreen mode.");
            
            _resizing = true;
            
            _graphics.PreferredBackBufferWidth = GraphicsAdapter.DefaultAdapter.CurrentDisplayMode.Width;
            _graphics.PreferredBackBufferHeight = GraphicsAdapter.DefaultAdapter.CurrentDisplayMode.Height;
            _graphics.IsFullScreen = true;         
            _graphics.ApplyChanges();
            Fullscreen = true;

            _resizing = false;
        }
        
        public void SetWindowed(int width, int height)
        {
            if (width <= 0)
                throw new ArgumentException("Width must be greater than zero");
            
            if (height <= 0)
                throw new ArgumentException("Height must be greater than zero");
         
            
            Mononoke.Logger.Print($"Entering windowed mode. Resolution: {width}x{height}");
            
            _resizing = true;
            
            _graphics.PreferredBackBufferWidth = width;
            _graphics.PreferredBackBufferHeight = height;
            _graphics.IsFullScreen = false;
            _graphics.ApplyChanges();
            Fullscreen = false;

            UpdateView();
            
            _resizing = false;
        }
        
        
        
        
        
        private void UpdateView()
        {
            GraphicsDevice gd = Mononoke.Graphics.GraphicsDevice;
            if (gd == null)
                return;
            
            float screenWidth = gd.PresentationParameters.BackBufferWidth;
            float screenHeight = gd.PresentationParameters.BackBufferHeight;

            UpdateView(screenWidth, screenHeight);
        }
        
        private void UpdateView(float screenWidth, float screenHeight)
        {
            // get View Size
            if (screenWidth / Width > screenHeight / Height)
            {
                ViewWidth = (int)(screenHeight / Height * Width);
                ViewHeight = (int)screenHeight;
            }
            else
            {
                ViewWidth = (int)screenWidth;
                ViewHeight = (int)(screenWidth / Width * Height);
            }

            // apply View Padding
            var aspect = ViewHeight / (float)ViewWidth;
            //ViewWidth -= ViewPadding * 2;
            //ViewHeight -= (int)(aspect * ViewPadding * 2);

            // update screen matrix
            ScreenMatrix = Matrix.CreateScale(ViewWidth / (float)Width);

            // update viewport
            Viewport = new Viewport
            {
                X = (int)(screenWidth / 2 - ViewWidth / 2f),
                Y = (int)(screenHeight / 2 - ViewHeight / 2f),
                Width = ViewWidth,
                Height = ViewHeight,
                MinDepth = 0,
                MaxDepth = 1
            };

            //Debug Log
            //Calc.Log("Update View - " + screenWidth + "x" + screenHeight + " - " + viewport.Width + "x" + viewport.GuiHeight + " - " + viewport.X + "," + viewport.Y);
            
        }
        
        
      
        
        
        
        private void OnClientSizeChanged(object sender, EventArgs e)
        {
            MononokeGame game = MononokeGame.Instance;
            
            if (game.Window.ClientBounds.Width > 0 && game.Window.ClientBounds.Height > 0 && !_resizing)
            {
                _resizing = true;

                _graphics.PreferredBackBufferWidth = game.Window.ClientBounds.Width;
                _graphics.PreferredBackBufferHeight = game.Window.ClientBounds.Height;
                UpdateView();

                _resizing = false;
            }
        }
        
        
        private void OnGraphicsReset(object sender, EventArgs e)
        {
            UpdateView();

            //if (scene != null)
            //    scene.HandleGraphicsReset();
        }

        private void OnGraphicsCreate(object sender, EventArgs e)
        {
            UpdateView();

            //if (scene != null)
            //    scene.HandleGraphicsCreate();
        }
        
        
    }
}