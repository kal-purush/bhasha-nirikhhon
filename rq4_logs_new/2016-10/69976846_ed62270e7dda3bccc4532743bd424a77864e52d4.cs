using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xna.Framework;
using OpenTK.Input;

namespace FlappyBirdTeam.GameObjects
{
    public class Bird
    {
        private Vector2 _position;
        private Vector2 _velocity;
        private Vector2 _size;
        public Vector2 Position {get { return _position; } set { _position = value; }}
        public Vector2 Velocity { get { return _velocity; } set { _velocity = value; } }
        public Vector2 Size { get { return _size; } set { _size = value; } }

        public Bird()
        {
            _position = new Vector2(200, 0);
            _velocity = new Vector2(0, 200.0f);
            _size = new Vector2(30, 30);
        }

        public void Update(GameTime time)
        {
            _velocity.Y += 9.8f;
            if (_velocity.Y > 400)
                _velocity.Y = 400;

            _position.Y += (float)time.ElapsedGameTime.TotalSeconds*_velocity.Y;
        }

        public void OnTouch()
        {
            _velocity.Y = -200;
        }
    }
}