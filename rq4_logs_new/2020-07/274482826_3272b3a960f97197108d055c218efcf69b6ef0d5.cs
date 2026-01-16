using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Content;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;
using System;

namespace PixelHunter1995
{
    class Player : IUpdateable, IDrawable
    {

        Texture2D sprite;
        Vector2 Position { get; set; }
        Vector2 MovePosition { get; set; }
        //private readonly SpriteBatch spriteBatch;
        private readonly Game game;

        public Player(Game game, SpriteBatch spriteBatch)
        {
            this.game = game;
            //this.spriteBatch = spriteBatch;

            this.Position = new Vector2(50, 50);
            this.MovePosition = this.Position;
        }

        /// <summary>
        /// LoadContent will be called once per game and is the place to load
        /// all of your content.
        /// </summary>
        public void LoadContent(ContentManager content)
        {
            sprite = content.Load<Texture2D>("Images/snubbe");
        }

        public void Update(GameTime gameTime)
        {
            var mouseState = Mouse.GetState();

            if (game.IsActive && mouseState.LeftButton == ButtonState.Pressed)
            {
                this.MovePosition = new Vector2(mouseState.X, mouseState.Y);
            }

            this.Position = this.Approach(Position, MovePosition, 2);

        }

        public void Draw(SpriteBatch spriteBatch)
        {
            spriteBatch.Draw(sprite, Position, Color.White);
        }

        public Vector2 Approach(Vector2 start, Vector2 target, double speed)
        {
            Vector2 error = target - start;
            if (error.LengthSquared() <= Math.Pow(speed,2))
            {
                return target;
            }
            else
            {
                Vector2 dir = Vector2.Normalize(error);
                //return start + new Vector2(dir.X * speed, dir.Y * speed);
                return start + dir * (float) speed;
            }
        }
    }
}