// Button.cs
// James Tyson
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;

using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Content;
using Microsoft.Xna.Framework.Graphics;

using RkoOuttaNowhere.Images;
using RkoOuttaNowhere.Input;
using RkoOuttaNowhere.Screens;

namespace RkoOuttaNowhere.Ui
{
    public class Button
    {
        /// <summary>
        /// Differenct states the button can be in
        /// </summary>
        public enum ButtonState
        {
            Neutral,
            Over,
            Pressed
        }

        private ButtonState _state;
        private bool _maintainPressedState; //Used if the mouse presses down on button but then leaves the button, so if it re-enters it will re-press it
        private bool _visible;
        private Image _currentImage;
        private Rectangle _hitbox;
        private object _value; //  Additional value that can be stored in the Button

        public List<Image> Images; //Will hold one for each button state



        public event EventHandler ButtonClicked;
        protected virtual void OnButtonClicked(EventArgs e)
        {
            if (ButtonClicked != null)
                ButtonClicked(this, e);
        }
        

        public Rectangle HitBox
        {
            get { return _hitbox; }
            set { _hitbox = value; }
        }

        public object Value
        {
            get { return _value; }
            set { _value = value; }
        }
        
        public bool Visible
        {
            get { return _visible; }
            set { _visible = value; }
        }

        public Button()
        {
            Images = new List<Image>();
            _hitbox = Rectangle.Empty;
            _state = ButtonState.Neutral;
            _maintainPressedState = false;
            _currentImage = new Image();
            _visible = true;
        }

        public void LoadContent(string path, Vector2 position, EventHandler handle)
        {
            // Load neutral image
            Image i = new Image();
            i.Path = path + "Neutral";
            i.LoadContent();
            Images.Add(i);
            // Load hover image
            i = new Image();
            i.Path = path + "Hover";
            i.LoadContent();
            Images.Add(i);
            // Load clicked image
            i = new Image();
            i.Path = path + "Clicked";
            i.LoadContent();
            Images.Add(i);

            _currentImage = Images[0];
            _hitbox = new Rectangle((int)position.X, (int)position.Y, _currentImage.SourceRect.Width, _currentImage.SourceRect.Height);
            ButtonClicked = handle;
            Value = _currentImage.Text;
        }

        public void UnloadContent()
        {
            foreach (Image i in Images)
                i.UnloadContent();
        }

        public void Update(GameTime gameTime)
        {
            bool isMouseDown = InputManager.Instance.LeftMouseDown();
            Point location = InputManager.Instance.MousePosition + ScreenManager.Instance.Camera.WorldPosition;
            bool isMouseOver = _hitbox.Contains(location);

            //Clear the pressed state variable if click is released outside of button
            if (!isMouseDown && _maintainPressedState)
                _maintainPressedState = false;

            // Check for button state changes
            switch (_state)
            {
                case ButtonState.Neutral:
                    if (isMouseOver)
                    {
                        if (_maintainPressedState)
                            _state = ButtonState.Pressed;
                        else
                            _state = ButtonState.Over;
                        SetCurrentImage();
                    }
                    break;
                case ButtonState.Over:
                    if (!isMouseOver)
                    {
                        _state = ButtonState.Neutral;
                        SetCurrentImage();
                    }
                    else
                    {
                        if (isMouseDown)
                        {
                            _maintainPressedState = true;
                            _state = ButtonState.Pressed;
                            SetCurrentImage();
                        }
                    }
                    break;
                case ButtonState.Pressed:

                    if (!isMouseDown)
                    {
                        //Click
                        if (isMouseOver)
                            _state = ButtonState.Over;
                        else
                            _state = ButtonState.Neutral;
                        OnButtonClicked(new EventArgs());
                        SetCurrentImage();
                    }
                    else if (!isMouseOver)
                    {
                        _state = ButtonState.Neutral;
                        SetCurrentImage();
                    }
                    break;
            }

            // Account for camera motion
            if (ScreenManager.Instance.Camera.WorldChange != Vector2.Zero)
            {
                _currentImage.Position -= ScreenManager.Instance.Camera.WorldChange;
            }
        }

        public void Draw(SpriteBatch spriteBatch)
        {
            if(_visible)
                _currentImage.Draw(spriteBatch);
        }

        /// <summary>
        /// Sets the current image based on the button state
        /// </summary>
        private void SetCurrentImage()
        {
            Vector2 pos = _currentImage.Position;
            switch (_state)
            {
                case ButtonState.Neutral:
                    _currentImage = Images[0];
                    break;
                case ButtonState.Over:
                    _currentImage = Images[1];
                    break;
                case ButtonState.Pressed:
                    _currentImage = Images[2];
                    break;
            }
            _currentImage.Position = pos;
        }
    }
}