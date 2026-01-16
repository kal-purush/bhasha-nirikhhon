using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Platformer
{
    class MenuItem
    {
        public string Text;
        public string TargetScreen;

        public Color TextColor { get; set; }
        public Color BorderColor { get; set; }
        public Color BackgroundColor { get; set; }

        public Rectangle Area { get; set; }
        public Vector2 Position { get; set; }
        public Rectangle Border { get; set; }

        public MenuItem(string text, string targetScreen)
        {
            this.Text = text;
            this.TargetScreen = targetScreen;
        }

        public void Update(GameTime gameTime) {
            
        }

        public void Draw(SpriteBatch sb)
        {
            //sb.DrawString(font, menuItems[0][0], new Vector2(100, 100), Color.Black);
        }

        public void Highlight()
        {

        }
    }
}