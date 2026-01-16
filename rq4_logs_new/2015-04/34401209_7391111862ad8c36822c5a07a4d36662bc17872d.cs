using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Audio;
using Microsoft.Xna.Framework.Content;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;
using Microsoft.Xna.Framework.Media;
using System.Diagnostics;

namespace City_Builder
{
    /// <summary>
    /// This is the main type for your game
    /// </summary>
    public class City_Builder : Microsoft.Xna.Framework.Game
    {
        GraphicsDeviceManager graphics;
        SpriteBatch spriteBatch;
        private bool active;
        GameState state;
        Playing playing;
        Initializer init;

        public City_Builder()
        {
            graphics = new GraphicsDeviceManager(this);
            graphics.PreferredBackBufferWidth = 1360;
            graphics.PreferredBackBufferHeight = 960;
            graphics.IsFullScreen = false;
            graphics.ApplyChanges();
            Content.RootDirectory = "Content";
            Window.AllowUserResizing = false;
        }

        protected override void Initialize()
        {
            // TODO: Add your initialization logic here

            base.Initialize();
        }
        protected override void OnDeactivated(object sender, EventArgs args)
        {
            active = false;
            base.OnDeactivated(sender, args);
        }
        protected override void OnActivated(object sender, EventArgs args)
        {
            active = true;
            base.OnActivated(sender, args);
        }
        public bool isActive() { return active; }

        protected override void LoadContent()
        {
            // Create a new SpriteBatch, which can be used to draw textures.
            //AspectRatio = GraphicsDevice.Viewport.AspectRatio;
            //OldWindowSize = new Point(Window.ClientBounds.Width, Window.ClientBounds.Height);
            spriteBatch = new SpriteBatch(GraphicsDevice);
            init = new Initializer(spriteBatch, this);
            state = init;
        }

        public void changeGameState(string newState)
        {
            if(newState.Equals("playing"))
            {
                playing = new Playing(spriteBatch, this);
                state.leaving();
                state = playing;
                state.entering();
            }
            if (newState.Equals("initialize"))
            {
                init = new Initializer(spriteBatch, this);
                state.leaving();
                state = init;
                state.entering();
            }
        }

        protected override void UnloadContent()
        {
            // TODO: Unload any non ContentManager content here
        }

        protected override void Update(GameTime gameTime)
        {
            // Allows the game to exit
            if (GamePad.GetState(PlayerIndex.One).Buttons.Back == ButtonState.Pressed)
                this.Exit();
            //

            if (active) 
            {
                state.update(gameTime);
            }

            // TODO: Add your update logic here

            base.Update(gameTime);
        }

        protected override void Draw(GameTime gameTime)
        {
            GraphicsDevice.Clear(Color.Black);
            spriteBatch.Begin();

            state.draw();
            
            spriteBatch.End();
            base.Draw(gameTime);
        }
    }
}