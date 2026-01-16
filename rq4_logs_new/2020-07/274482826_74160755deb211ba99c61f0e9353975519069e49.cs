using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;
using System;
using System.Runtime.InteropServices;

namespace PixelHunter1995
{
    /** This class is responsible for switching between windowed and fullscreen mode.
     */
    internal class Screen
    {
        internal int fullScreenWidth;
        internal int fullScreenHeight;
        public Rectangle renderTargetRect;
        private readonly GraphicsDeviceManager graphics;
        private readonly GameWindow window;
        private bool justToggledFullscreen = false;

        public Screen(GraphicsDeviceManager graphics, GameWindow window)
        {
            this.graphics = graphics;
            this.window = window;
            graphics.PreferredBackBufferWidth = GlobalSettings.WINDOW_WIDTH;
            graphics.PreferredBackBufferHeight = GlobalSettings.WINDOW_HEIGHT;
            Initialize();
        }

        public void SetFullScreenResolution()
        {
            fullScreenWidth = GraphicsAdapter.DefaultAdapter.CurrentDisplayMode.Width;
            fullScreenHeight = GraphicsAdapter.DefaultAdapter.CurrentDisplayMode.Height;
        }

        private void SetToWindowed()
        {
            renderTargetRect = RenderTargetWindowRect();
            graphics.PreferredBackBufferWidth = GlobalSettings.WINDOW_WIDTH;
            graphics.PreferredBackBufferHeight = GlobalSettings.WINDOW_HEIGHT;
            graphics.ApplyChanges();
            window.IsBorderless = false;
            window.Position = new Point((fullScreenWidth - GlobalSettings.WINDOW_WIDTH) / 2,
                                        (fullScreenHeight - GlobalSettings.WINDOW_HEIGHT) / 2);

        }

        private Rectangle RenderTargetWindowRect()
        {
            return new Rectangle(0, 0, GlobalSettings.WINDOW_WIDTH, GlobalSettings.WINDOW_HEIGHT);
        }

        internal void Initialize()
        {
            SetFullScreenResolution();
            SetToWindowed();
        }

        public void ToggleFullScreen()
        {
            if (window.IsBorderless)
                SetToWindowed();
            else
                SetToFullScreen();
        }

        private void SetToFullScreen()
        {
            renderTargetRect = RenderTargetFullScreenRect();
            graphics.PreferredBackBufferWidth = fullScreenWidth;
            graphics.PreferredBackBufferHeight = fullScreenHeight;
            graphics.ApplyChanges();
            window.IsBorderless = true;
            window.Position = new Point(0, 0);

        }

        private Rectangle RenderTargetFullScreenRect()
        {
            if (fullScreenWidth > fullScreenHeight)
            {
                int newWindowWidth = (int)System.Math.Ceiling(
                    ((float)fullScreenHeight / (float)GlobalSettings.WINDOW_HEIGHT) * GlobalSettings.WINDOW_WIDTH);

                return new Rectangle((fullScreenWidth - newWindowWidth) / 2,
                    0, newWindowWidth, fullScreenHeight);
            }
            else
            {
                int newWindowHeight = (int)System.Math.Ceiling(
                    ((float)fullScreenWidth / (float)GlobalSettings.WINDOW_WIDTH) * GlobalSettings.WINDOW_HEIGHT);

                return new Rectangle(0, (fullScreenHeight - newWindowHeight) / 2,
                    fullScreenWidth, newWindowHeight);
            }
        }

        private bool IsAltEnterPressed(KeyboardState state)
        {
            return (state.IsKeyDown(Keys.LeftAlt) || state.IsKeyDown(Keys.RightAlt)) && state.IsKeyDown(Keys.Enter);
        }

        public void CheckForFullScreen()
        {
            KeyboardState state = Keyboard.GetState();
            if (!justToggledFullscreen && IsAltEnterPressed(state))
            {
                ToggleFullScreen();
                justToggledFullscreen = true;
            }

            if (!IsAltEnterPressed(state))
            {
                justToggledFullscreen = false;
            }
        }

    }
}