using System;
using FlappyBirdTeam.Tools;
using Microsoft.Xna.Framework;

namespace FlappyBirdTeam.GameObjects
{
    public abstract class GameObject
    {
        protected Vector2 _acceleration;
        protected Vector2 _position;
        protected Vector2 _velocity;
        protected Vector2 _size;

        public Vector2 Position { get { return _position; } set { _position = value; } }
        public Vector2 Velocity { get { return _velocity; } set { _velocity = value; } }
        public Vector2 Size { get { return _size; } set { _size = value; } }

        protected GameObject(Single x, Single y, Single width, Single height)
        {
            _position = new Vector2(x, y);
            _size = new Vector2(width, height);

            InputHandler.Touch += OnTouch;
        }

        protected abstract void OnTouch(Object sender, EventArgs e);

        public virtual void Update(GameTime time)
        {
            _position += (Single)time.ElapsedGameTime.TotalSeconds * _velocity;
        }

        public virtual void Dispose()
        {
            InputHandler.Touch -= OnTouch;
        }
    }
}