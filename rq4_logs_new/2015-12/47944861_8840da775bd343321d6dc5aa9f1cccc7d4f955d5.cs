using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Content;
using Microsoft.Xna.Framework.Input;

namespace Tphx.Bicephalisnake
{
    class MainMenu : GameComponent
    {
        private Texture2D texture;
        private double timeSinceLastInput;

        public MainMenu(ContentManager content)
            : base(content)
        {
            this.texture = this.content.Load<Texture2D>("Textures\\MainMenu");
        }

        public override void Draw(SpriteBatch spriteBatch)
        {
            spriteBatch.Draw(this.texture, Vector2.Zero, Color.White);
        }

        public override void Update(GameTime gameTime)
        {
            this.timeSinceLastInput += gameTime.ElapsedGameTime.TotalSeconds;

            if (this.timeSinceLastInput >= 2.0)
            {
                KeyboardState keyboardState = Keyboard.GetState();

                if (keyboardState.IsKeyDown(Keys.Left) || keyboardState.IsKeyDown(Keys.Down))
                {
                    this.StartGame = true;
                }
            }
        }

        public bool StartGame { get; private set; }
    }
}