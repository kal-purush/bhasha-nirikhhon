using SFML.Graphics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Presentation
{
    class Circle : CircleShape, Drawable
    {
        public int number { get; set; }
        Font f;
        Text t;

        public Circle(int number)
        {
            f = new Font(Environment.GetFolderPath(Environment.SpecialFolder.Fonts) + "\\arial.ttf");
            t = new Text(number.ToString(), f, 30);
            t.Color = Color.Yellow;
        }

        public Circle(float r, uint pointCount, int number) : this(number)
        {
            this.number = number;
            Radius = r;
            SetPointCount(pointCount);
            t.Origin = new SFML.System.Vector2f(-r + t.GetLocalBounds().Width / 2 + t.GetLocalBounds().Left, -r + t.GetLocalBounds().Height / 2  + t.GetLocalBounds().Top);
        }

        public new void Draw(RenderTarget target, RenderStates states)
        {
            base.Draw(target, states);
            target.Draw(t);
        }

        public new SFML.System.Vector2f Origin
        {
            get { return base.Origin; }
            set
            {
                base.Origin = value;
                t.Origin += value;
            }
        }

        public new SFML.System.Vector2f Position
        {
            get { return base.Position; }
            set
            {
                base.Position = value;
                t.Position = value;
            }
        }

    }
}