using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using TrinityCore.GameComponents;

namespace TrinityCore.Screens
{
    public sealed class TestScreen : Screen
    {
        private Game game;
        private SpriteBatch sb;

        private Dictionary<String, Texture2D> textures; 

        public TestScreen(Game game, ScreenManager screenManager)
            : base(game, screenManager, "test screen")
        {
            this.game = game;

            textures = new Dictionary<string, Texture2D>();
            Components.Add(new FPSCounter(game));

            LoadContent();
        }

        protected override void LoadContent()
        {
            base.LoadContent();
            sb = new SpriteBatch(game.GraphicsDevice);

            textures.Add("hero", game.Content.Load<Texture2D>("textures/hero"));
        }

        public override void Update(GameTime gameTime)
        {
            InputHandler.Update();

            if (InputHandler.MouseLeftPressDown())
            {
                var scr = new Screen(game, ScreenManager, "anotherScreen", true);
                ScreenManager.PushScreen(scr);
            }
            if (InputHandler.MouseRightPressDown())
            {
                ScreenManager.PopScreen();
            }
        }

        public override void Draw(GameTime gameTime)
        {
            var texture = textures["hero"];
            
            sb.Begin();

            sb.Draw(texture, Vector2.Zero, Color.White);

            sb.End();

            base.Draw(gameTime);
        }
    }
}