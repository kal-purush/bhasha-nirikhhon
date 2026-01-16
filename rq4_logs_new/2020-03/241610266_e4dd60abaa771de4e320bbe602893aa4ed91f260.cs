using AsteroidGame.VisualObjects.Interfaces;
using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AsteroidGame.VisualObjects
{
    public class AidKit : ImageObject, ICollision
    {
        public int Power { get; set; } = 10;

        public AidKit(Point Position, Point Direction, Size AidKitSize) : base(Position, Direction, AidKitSize, Properties.Resources.aid_kit)
        {
        }

        public bool CheckCollision(ICollision obj) => Rect.IntersectsWith(obj.Rect);

        public override void Update()
        {
            _Position = new Point(_Position.X + _Direction.X, _Position.Y);
            if (_Position.X < -_Size.Width)
            {
                _Position = new Point(Game.Width + _Size.Width + Game.rand.Next(0, 200), Game.rand.Next(0, Game.Height));
            }
        }

    }
}