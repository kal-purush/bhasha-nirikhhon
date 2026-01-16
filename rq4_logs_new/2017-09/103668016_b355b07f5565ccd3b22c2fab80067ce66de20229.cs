using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;
using System;
namespace Pong
{
    public class Ball
    {
        public Vector2 position;
        public Vector2 direction;
        public Texture2D tex;
        public float speed;
        public Vector2 size;
        public Vector2 origin;
        public Ball(Vector2 position_, Vector2 direction_, float speed_, Texture2D tex_, Vector2 size_) {
            position = position_;
            direction = direction_;
            speed = speed_;
            tex = tex_;
            size = size_;
            origin = new Vector2(tex.Width / 2, tex.Height / 2);
        }
    }
}