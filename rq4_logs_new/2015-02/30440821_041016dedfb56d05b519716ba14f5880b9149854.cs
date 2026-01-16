using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;
using RkoOuttaNowhere.Images;
using RkoOuttaNowhere.Input;
using RkoOuttaNowhere.Screens;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RkoOuttaNowhere.Gameplay
{
    public class Player : GameObject
    {
        public Player() : base()
        {
            position = Vector2.Zero;
            image = new Image();
        }

        public void LoadContent() 
        { 
            image.Path = "Gameplay/player";
            image.Position = position;
            image.LoadContent();
        }
        public void UnloadContent() 
        {
            image.UnloadContent();
        }
        public void Update(GameTime gametime) 
        {

            if (InputManager.Instance.KeyDown(Keys.Left) && position.X > 4)
            {
                position.X-=5;
            }
            else if (InputManager.Instance.KeyDown(Keys.Right) && position.X < ScreenManager.Instance.Dimensions.X - image.SourceRect.Width)
            {
                position.X+=5;
            }
            else if (InputManager.Instance.KeyDown(Keys.Up) && position.Y > 4)
            {
                position.Y-=5;
            }
            else if (InputManager.Instance.KeyDown(Keys.Down) && position.Y < ScreenManager.Instance.Dimensions.Y-image.SourceRect.Height)
            {
                position.Y+=5;
            }
            image.Position = position;
            image.Update(gametime);
        }
        public void Draw(SpriteBatch spritebatch) 
        {
            image.Draw(spritebatch);
        }
    }
}