using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework;

namespace GD327_Final_Project
{
    public class Explosion : Sprite
    {

        public Explosion(Game1 game, Texture2D texture, Vector2 position, SpriteBatch spritebatch) : base(game, texture, position, spritebatch) { }

        public override void Update(GameTime gameTime)
        {
            base.Update(gameTime);
            Opacity -= .001f * (float)gameTime.ElapsedGameTime.TotalMilliseconds / 10;
            Scale *= 1 + .007f * (float)gameTime.ElapsedGameTime.TotalMilliseconds;
            if (Scale > 3)
            {
                this.Enabled = false;
                this.Visible = false;
            }
        }

    }
}