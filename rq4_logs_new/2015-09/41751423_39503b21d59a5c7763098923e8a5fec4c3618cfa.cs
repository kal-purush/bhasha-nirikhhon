using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Audio;
using Microsoft.Xna.Framework.Content;
using Microsoft.Xna.Framework.GamerServices;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;
using Microsoft.Xna.Framework.Media;

namespace mst_boredom_remover
{
    public class TextDisplay : UIObject
    {
        private Texture2D texture;
        private Vector2 position;
        private SpriteFont font;
        private Rectangle bounds;
        private Color color;

        private string text = "";
        private Vector2 textPosition;

        private int test = 1;

        public TextDisplay(Texture2D texture, Vector2 position, SpriteFont font, Color color)
            : base(texture, position)
        {
            this.texture = texture;
            this.position = position;
            this.font = font;
            this.color = color;
            calculateCenter();
        }

        private void calculateCenter()
        {
            if (text != "") // not empty string
            {
                // centers the string inside the texture
                textPosition = new Vector2((position.X + texture.Width / 2) - font.MeasureString(text).X / 2, 
                    (position.Y + texture.Height / 2) - font.MeasureString(text).Y / 2);
            }
            else
            {
                textPosition = position;
            }
        }

        public void changeText(string newText)
        {
            text = newText;
            calculateCenter();
        }

        public override void Update(GameTime gt)
        {
            test++;
            changeText(test.ToString());
            //base.Update(gt);
        }
        public override void Draw(SpriteBatch sb)
        {
            sb.Draw(texture, position, Color.White);
            sb.DrawString(font, text, textPosition, color);
            //base.Draw(sb);
        }
    }
}