using System;
using System.Collections.Generic;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;

namespace GSeoFinalProject
{
    class Rock
    {
        Game1 game;
        private Rectangle rectangle;
        Vector2 rockPosition;

        Texture2D rockTexture; //current rock's image

        Random random = new Random();

        public Rectangle Rectangle { get => rectangle; set => rectangle = value; }

        public Rock(Game1 game, Vector2 startposition)
        {
            this.game = game;
            rockPosition = startposition;
            rockTexture = game.Content.Load<Texture2D>("Images/rock");
            rectangle = new Rectangle((int)startposition.X, (int)startposition.Y, rockTexture.Width, rockTexture.Height);
        }
        
        /// <summary>
        /// Rocks are only going down
        /// </summary>
        public void Update()
        {
            rockPosition.Y += 1f; //all rock will get down
            rectangle.Y = (int)rockPosition.Y;

            // if an rock collide the fighter, the fighter's heart should reduce
            if (rectangle.Intersects(Fighter.rectangle))
            {
                Fighter.isHit = true;
            }
        }
        public void Draw(SpriteBatch spriteBatch)
        {
            spriteBatch.Draw(rockTexture, rockPosition, Color.White);
        }
    }
}