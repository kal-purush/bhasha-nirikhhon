using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Content;
using Microsoft.Xna.Framework.Graphics;

using RkoOuttaNowhere.Images;

namespace RkoOuttaNowhere.Gameplay
{
    public class GameObject
    {
        protected Vector2 _position, _velocity;
        protected Image _image;
        protected bool _isVisible, _isActive;

        public GameObject()
        {
            _position = _velocity = Vector2.Zero;
            _image = new Image();
            _isVisible = _isActive = true;
        }

        public virtual void LoadContent()
        {

        }

        public virtual void UnloadContent()
        {
            _image.UnloadContent();
        }

        public virtual void Update(GameTime gametime)
        {
            _image.Position = _position;
        }

        public virtual void Draw(SpriteBatch spritebatch)
        {
            _image.Draw(spritebatch);
        }
    }
}