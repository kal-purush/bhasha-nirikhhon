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
    class TextInput : UIObject
    {
        private Texture2D texture;
        private Texture2D activeTexture;
        private Vector2 position;
        private SpriteFont font;

        private string currentText = "";
        private bool active = true;

        // stores the last mouse state
        private MouseState previousState;
        private KeyboardState previousKeys;
        private Rectangle bounds;

        //public event EventHandler newInput;

        public TextInput(Texture2D texture, Texture2D activeTexture, Vector2 position, SpriteFont font)
            : base(texture, position)
        {
            this.texture = texture;
            this.activeTexture = activeTexture;
            this.position = position;
            this.font = font;

            bounds = new Rectangle((int)position.X, (int)position.Y, texture.Width, texture.Height);
        }

        public override void changeContext(int id)
        {

            //base.changeContext(id);
        }
        public override void Update(GameTime gt)
        {
            MouseState mouse = Mouse.GetState();
            KeyboardState keys = Keyboard.GetState();

            bool isMouseOver = bounds.Contains(mouse.X, mouse.Y); // check if the mouse is touching the button
            // first determine whether the text field needs to be activated or deactivated
            if (isMouseOver)
            {
                // if clicked on the field
                if (previousState.LeftButton == ButtonState.Pressed && mouse.LeftButton == ButtonState.Released)
                {
                    // activate the field
                    active = true;
                }
            }
            else
            {
                // if clicked anywhere but the field
                if (previousState.LeftButton == ButtonState.Pressed && mouse.LeftButton == ButtonState.Released)
                {
                    // deactivate the field
                    active = false;
                }
            }

            // only update the string if the field is active
            if (active)
            {
                // key input from user
                char c = ' ';
                foreach (Keys k in previousKeys.GetPressedKeys())
                {
                    if (keys.IsKeyUp(k))
                    {
                        if (k == Keys.Back)
                        {
                            c = '<';
                        }
                        else
                        {
                            try
                            {
                                c = Convert.ToChar(k);
                            }
                            catch (Exception e)
                            {

                            }
                        }
                    }
                }

                // if key was backspace
                if (c == '<')
                {
                    if (currentText.Length > 0)
                    {
                        currentText = currentText.Remove(currentText.Length - 1);
                    }
                }
                else
                {
                    // verify string length
                    if (font.MeasureString((currentText + "W")).X < texture.Width - 10)
                    {
                        // verify string contents
                        if (c != ' ' && (c > 65 && c <= 90) || (c > 96 && c <= 122))
                        {
                            currentText += c;
                        }
                    }
                }
            }
            
            previousState = mouse;
            previousKeys = keys;
            //base.Update(gt);
        }
        public override void Draw(SpriteBatch sb)
        {
            if (!active)
            {
                sb.Draw(texture, position, Color.White);
            }
            else
            {
                sb.Draw(activeTexture, position, Color.White);
            }
            sb.DrawString(font, currentText, position + new Vector2(10, 12), Color.Black);
            //base.Draw(sb);
        }
    }
}