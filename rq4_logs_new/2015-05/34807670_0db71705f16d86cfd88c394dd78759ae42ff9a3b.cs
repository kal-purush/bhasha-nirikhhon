using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SeriousGame
{
    class Obstacle
    {
        private string          _question;
        private string[]        _choices;

        private Rectangle       _hitBox;
        private Vector2         _position;
        private Color           _color;
        private Texture2D       _texture;
       

        public Obstacle (Color color, Vector2 position, Texture2D texture, string[] choices, string question)
        {
            _question = question;
            for (int i = 0; i <= choices.Length; i++)
            {
                _choices[i] = choices[i];
            }
            _position = position;
            _color = color;
            _texture = texture;
        }

        public void Update()
        {

        }

        public void Draw(SpriteBatch spriteBatch)
        {
            spriteBatch.Draw(_texture, _hitBox, _color);
        }

        public Rectangle getCollision()
        {
            return _hitBox;
        }

        public void openQuestion ()
        {

        }
    }
}