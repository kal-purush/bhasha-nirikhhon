using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using RkoOuttaNowhere.Images;
using RkoOuttaNowhere.Input;
using RkoOuttaNowhere.Screens;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RkoOuttaNowhere.Gameplay
{
    class Laser : Projectile
    {  
        public Laser(Vector2 start, Vector2 dest, int dmg)
        {
            _position = start;
            _velocity = dest - start;
            if (_velocity != Vector2.Zero)
                _velocity.Normalize();
            _image = new Image();
            Damage = dmg;
        }

        public void LoadContent() 
        {
            _image.Path = "Gameplay/Laser";
            _image.Position = _position;
            _image.LoadContent();
        }

        public void UnloadContent() 
        {
            _image.UnloadContent();
        }

        public void Update(GameTime gametime) 
        {
            _image.Position += _velocity * _speed * (float)gametime.ElapsedGameTime.TotalSeconds;
            _image.Update(gametime);            
        }

        public void Draw(SpriteBatch spritebatch) 
        {
            _image.Draw(spritebatch);
        }

    }
}