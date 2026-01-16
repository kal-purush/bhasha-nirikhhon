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
    public enum ButtonStatus
    {
        Normal, // button's regular look, no interaction
        MouseOver, // when a mouse is on the button
        Pressed, // when the button is clicked
    }
    public class Button
    {
        Texture2D texture;
        public bool visible = true;
        Vector2 position;
        // stores the last mouse state
        private MouseState previousState;

        // different textures
        private Texture2D hoverTexture;
        private Texture2D pressedTexture;

        // rectangle that covers the button
        private Rectangle bounds;

        // current button state
        private ButtonStatus state = ButtonStatus.Normal;

        public ButtonStatus GetButtonStatus
        {
            get { return state; }
        }

        // event upon being pressed
        public event EventHandler Clicked;

        // event upon being held down
        public event EventHandler OnPress;

        // button constructor
        public Button(Texture2D texture, Texture2D hoverTexture, Texture2D pressedTexture, Vector2 position)
        {
            this.texture = texture;
            this.hoverTexture = hoverTexture;
            this.pressedTexture = pressedTexture;
            this.position = position;

            this.bounds = new Rectangle((int)position.X, (int)position.Y, texture.Width, texture.Height); // set the bounds
        }


        public void Update(GameTime gameTime) // update method for buttons
        {
            if (visible)
            {
                // tracks mouse position
                MouseState mouseState = Mouse.GetState();

                int MouseX = mouseState.X; // sets mouse x position
                int MouseY = mouseState.Y; // sets mouse y position

                bool isMouseOver = bounds.Contains(MouseX, MouseY); // check if the mouse is touching the button

                // update the button state
                if (isMouseOver && state != ButtonStatus.Pressed)
                {
                    state = ButtonStatus.MouseOver; // button uses the mouseover state
                }
                // if the button state does not have a mouse over it and has not been clicked
                else if (!isMouseOver && state != ButtonStatus.Pressed)
                {
                    state = ButtonStatus.Normal; // button uses the normal button state
                }

                // check if player holds down button
                if (mouseState.LeftButton == ButtonState.Pressed && previousState.LeftButton == ButtonState.Released)
                {
                    if (isMouseOver)
                    {
                        // update the button state
                        state = ButtonStatus.Pressed;

                        if (OnPress != null)
                        {
                            // do the OnPress event
                            OnPress(this, EventArgs.Empty);
                        }
                    }
                }

                // check if the player releases the button
                if (mouseState.LeftButton == ButtonState.Released && previousState.LeftButton == ButtonState.Pressed)
                {
                    if (isMouseOver)
                    {
                        // update the button state
                        state = ButtonStatus.MouseOver;

                        if (Clicked != null)
                        {
                            // do the clicked event
                            Clicked(this, EventArgs.Empty);
                        }
                    }
                    // if the button has been clicked
                    else if (state == ButtonStatus.Pressed)
                    {
                        state = ButtonStatus.Normal;
                    }
                }
                previousState = mouseState;
            }
        } // end update method
        public void Draw(SpriteBatch spriteBatch, int special = 0)
        {
            switch (special)
            {
                case 0:
                    if (visible)
                    {
                        // draw the button using a switch on the status of the button
                        switch (state)
                        {
                            // draw the normal state of the button
                            case ButtonStatus.Normal:
                                spriteBatch.Draw(texture, new Rectangle((int)position.X, (int)position.Y, 100, 35), Color.White);
                                break;
                            // draw the mouseover state of the button
                            case ButtonStatus.MouseOver:
                                spriteBatch.Draw(hoverTexture, new Rectangle((int)position.X, (int)position.Y, 100, 35), Color.White);
                                break;
                            // draw the pressed state of the button
                            case ButtonStatus.Pressed:
                                spriteBatch.Draw(pressedTexture, new Rectangle((int)position.X, (int)position.Y, 100, 35), Color.White);
                                break;
                            default:
                                spriteBatch.Draw(texture, new Rectangle((int)position.X, (int)position.Y, 100, 35), Color.White);
                                break;
                        }
                    }
                    break;
                case 1:
                    if (visible)
                    {
                        // draw the button using a switch on the status of the button
                        switch (state)
                        {
                            // draw the normal state of the button
                            case ButtonStatus.Normal:
                                spriteBatch.Draw(texture, new Rectangle((int)position.X, (int)position.Y, 75, 75), Color.White);
                                //spriteBatch.Draw(texture, new Vector2((int)position.X, (int)position.Y), Color.White);
                                break;
                            // draw the mouseover state of the button
                            case ButtonStatus.MouseOver:
                                spriteBatch.Draw(hoverTexture, new Vector2((int)position.X, (int)position.Y), Color.White);
                                break;
                            // draw the pressed state of the button
                            case ButtonStatus.Pressed:
                                spriteBatch.Draw(pressedTexture, new Vector2((int)position.X, (int)position.Y), Color.White);
                                break;
                            default:
                                spriteBatch.Draw(texture, new Vector2((int)position.X, (int)position.Y), Color.White);
                                break;
                        }
                    }
                    break;
                case 2:
                    if (visible)
                    {
                        // draw the button using a switch on the status of the button
                        switch (state)
                        {
                            // draw the normal state of the button
                            case ButtonStatus.Normal:
                                spriteBatch.Draw(texture, new Rectangle((int)position.X, (int)position.Y, 25, 25), Color.White);
                                //spriteBatch.Draw(texture, new Vector2((int)position.X, (int)position.Y), Color.White);
                                break;
                            // draw the mouseover state of the button
                            case ButtonStatus.MouseOver:
                                spriteBatch.Draw(hoverTexture, new Vector2((int)position.X, (int)position.Y), Color.White);
                                break;
                            // draw the pressed state of the button
                            case ButtonStatus.Pressed:
                                spriteBatch.Draw(pressedTexture, new Vector2((int)position.X, (int)position.Y), Color.White);
                                break;
                            default:
                                spriteBatch.Draw(texture, new Vector2((int)position.X, (int)position.Y), Color.White);
                                break;
                        }
                    }
                    break;
            }
        }
        public void ChangeTexture(Texture2D texture)
        {
            this.texture = texture;
        }
    }
}