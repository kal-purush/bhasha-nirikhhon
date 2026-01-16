using System;
using System.Collections.Generic;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;

namespace GSeoFinalProject
{
    class EnemyShot
    {
        Game1 game;
        Rectangle rectangle;

        Texture2D shotTexture;

        public EnemyShot(Game1 game, Vector2 startLocation)
        {
            this.game = game;
            shotTexture = game.Content.Load<Texture2D>("Images/enemyShot");
            rectangle = new Rectangle((int)startLocation.X, (int)startLocation.Y, shotTexture.Width, shotTexture.Height);
        }

        public void Update()
        {
            if (rectangle.Intersects(Fighter.rectangle))
            {
                Fighter.isHit = true;
            }
            rectangle.Y += 30;
        }

        public void Draw(SpriteBatch spriteBatch)
        {
            if (rectangle.Y > shotTexture.Height * -1)
            {
                spriteBatch.Draw(shotTexture, rectangle, Color.White);

            }
        }
    }
}