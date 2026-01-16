using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Drawing;

namespace HeadSoccer
{
    public class Ball
    {
        public int x, y, xSpeed, ySpeed;

        public Ball(int _x, int _y, int _xSpeed, int _ySpeed)
        {
            _x = x;
            _y = y;
            _xSpeed = xSpeed;
            _ySpeed = ySpeed;
        }

        public void move(int b, int a)
        {
            y = a*(x * x) + x + b;
        }

        public bool BlockCollision(Player p)
        {
            Rectangle blockRec = new Rectangle(b.x, b.y, b.width, b.height);
            Rectangle ballRec = new Rectangle(x, y, size, size);

            if (ballRec.IntersectsWith(blockRec))
            {
                string side = CollisionSide(blockRec);
            }

            return blockRec.IntersectsWith(ballRec);
        }
    }
}