using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SFML.Graphics;

namespace Presentation
{
    class Line : VertexArray, Drawable
    {
        Font f;
        List<Text> tList;

        public Line()
        {
            PrimitiveType = PrimitiveType.Lines;
            f = new Font(Environment.GetFolderPath(Environment.SpecialFolder.Fonts) + "\\arial.ttf");
            tList = new List<Text>();
        }

        public Line(SFML.System.Vector2f x, SFML.System.Vector2f y, int number) :this()
        {
            Append(new Vertex(x));
            Append(new Vertex(y));

            Text tmp = new Text(number.ToString(), f, 20);
            tmp.Color = Color.White;
            tmp.Position = new SFML.System.Vector2f((x.X + y.X) / 2, (x.Y + y.Y) / 2);
            FloatRect ftmp = tmp.GetLocalBounds();
            tmp.Origin = tmp.Origin + new SFML.System.Vector2f(ftmp.Width/2 + ftmp.Left, ftmp.Height/2 + ftmp.Top);
            tList.Add(tmp);
        }
        
        public new void Draw(RenderTarget target, RenderStates states)
        {
            base.Draw(target, states);
            foreach (var item in tList)
            {
                target.Draw(item);
            }
        }
    }
}