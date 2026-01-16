using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using System;

namespace WindowsGame1
{
    class Ball
    {
        bool collided;
        int ballSpeed = 12;

        Vector2 spd, pos;
        Rectangle bounds, screenBounds;
        Texture2D texture;

        public Rectangle Bounds
        {
            get
            {
                bounds.X = (int)pos.X;
                bounds.Y = (int)pos.Y;
                return bounds;
            }
        }

        public Ball(Texture2D t, Rectangle r)
        {
            bounds = new Rectangle(0, 0, t.Width, t.Height);
            texture = t;
            screenBounds = r;
        }

        public void setDefault(Rectangle r)
        {
            Random rand = new Random();
            spd = new Vector2(rand.Next(2, 6), -rand.Next(2, 6));
            spd.Normalize();
            pos.Y = r.Y - texture.Height;
            pos.X = r.X + (r.Width - texture.Width) / 2;
        }

        public bool Update()
        {
            collided = false;
            pos += spd * ballSpeed;
            return CheckWallCollision();
        }

        private bool CheckWallCollision()
        {
            if (pos.X < 0)
            {
                pos.X = 0;
                spd.X *= -1;
                return true;
            }
            if (pos.X + texture.Width > screenBounds.Width)
            {
                pos.X = screenBounds.Width - texture.Width;
                spd.X *= -1;
                return true;
            }
            if (pos.Y < 0)
            {
                pos.Y = 0;
                spd.Y *= -1;
                return true;
            }
            return false;
        }

        public bool checkBottom()
        {
            if (pos.Y > screenBounds.Height)
                return true;
            return false;
        }

        public bool PaddleCollision(Rectangle r)
        {
            Rectangle ballLocation = new Rectangle((int)pos.X, (int)pos.Y, texture.Width, texture.Height);
            if (r.Intersects(ballLocation))
            {
                pos.Y = r.Y - texture.Height;
                spd.Y *= -1;
                return true;
            }
            return false;
        }

        public void Deflection()
        {
            if (!collided)
            {
                spd.Y *= -1;
                collided = true;
            }
        }

        public void Draw(SpriteBatch sb)
        {
            sb.Draw(texture, pos, Color.White);
        }
    }
}