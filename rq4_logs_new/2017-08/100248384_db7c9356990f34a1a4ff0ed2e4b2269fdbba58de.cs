using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;

namespace Game1.GameObjects
{
    public class GameObject
    {

        private Vector2 position;
        private Rectangle rect;
        private Texture2D tex;

        public Vector2 Position { get => position; set => position = value; }
        public Rectangle Rect { get => rect; set => rect = value; }

        public virtual void LoadContent()
        {

        }

        public virtual void UnloadContent()
        {

        }

        public virtual void Update()
        {
            
        }

        public virtual void Draw()
        {

        }
    }
}