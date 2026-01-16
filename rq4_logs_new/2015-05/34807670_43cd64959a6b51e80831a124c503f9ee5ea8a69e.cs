using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Content;

namespace SeriousGame
{
    class ScreenManager
    {
        private static ScreenManager _instance;
        public Vector2 Dimensions { private set; get; }

        public Texture2D Bob;

        public static ScreenManager Instance
        {
            get
            {
                if (_instance == null)
                {
                    _instance = new ScreenManager();
                }
                return _instance;
            }
        }

        public ScreenManager()
        {
            Dimensions = new Vector2(1920, 1080);
        }
        
        public void LoadContent(ContentManager Content)
        {
            Bob = Content.Load<Texture2D>("bob");
        }

        public void UnloadContent()
        {

        }

        public void Update(GameTime gameTime, Game1 game)
        {
            System.Console.WriteLine("pizza");
            System.Console.WriteLine(game.GraphicsDevice.Viewport.Width);
        }

        public void Draw(SpriteBatch spriteBatch)
        {
            spriteBatch.Draw(Bob, new Vector2(0,0));
            spriteBatch.Draw(Bob, new Vector2(1900, 1000));
        }


    }
}