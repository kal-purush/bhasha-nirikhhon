using System.Windows.Media;
using System.Windows.Shapes;

namespace GameClient
{
    public class Shashka
    {
        public Ellipse form { get; set; }
        public int X { get; set; }
        public int Y { get; set; }
        public string Color { get; set; }
        public Shashka(string Color)
        {
            var bc = new BrushConverter();
            form = new Ellipse()
            {
                Stroke = (Brush)bc.ConvertFrom(Color),
                StrokeThickness = 60
            };
        }
        public Shashka()
        {

        }
    }
}