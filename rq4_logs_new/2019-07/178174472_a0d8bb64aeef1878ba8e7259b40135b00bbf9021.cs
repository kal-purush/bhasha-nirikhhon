using System;
using Microsoft.Xna.Framework;
using MononokeEngine.ECS;

namespace MononokeEngine.Physics
{
    public class Transform : Component
    {
        
        
        public Vector2 Position
        {
            get => _position;
            set
            {
                if (value == null)
                    throw new ArgumentException("Cannot set Position to null.");
                
                _position = value;
                if (Entity != null)
                    Entity.Scene.Space.AddEntityToChangeQueue(Entity);
            }
        }
        private Vector2 _position;
        
        
        
        public Vector2 Scale { get; }
        public float Rotation { get; }

        public bool Active => true;
        public bool Visible => false;
        public Entity Entity { get; set; }



        internal Transform()
        {
            _position = Vector2.Zero;
        }
        
        
        
        public void Update()
        {
            
        }

        public void Render()
        {
            
        }
    }
}