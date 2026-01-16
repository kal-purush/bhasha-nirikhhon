using System.Drawing;
using System.Windows.Forms;
using SummerTask_RadkevichMarsell.blockScheme.blocks;

namespace SummerTask_RadkevichMarsell.blockScheme
{
    public class OperationBlueprint : BlockBlueprint
    {
        private const int WIDTH_OFFSET = 10;
        private const int HEIGHT_OFFSET = 5;

        public OperationBlueprint(BlockTemplate block) : base(block)
        {
            ConfigureBlueprint(block);
        }

        private void ConfigureBlueprint(BlockTemplate block)
        {
            BackColor = defaultBackColor;
            var textSizePixels = TextRenderer.MeasureText(block.Content, defaultFont);
            Size = new Size(textSizePixels.Width + WIDTH_OFFSET, textSizePixels.Height + HEIGHT_OFFSET);
            DownOutput = new Point(Location.X + Size.Width / 2, Location.Y + Size.Height);
        }

        public override void Draw()
        {
            var bmp = new Bitmap(Size.Width, Size.Height);
            var graphics = Graphics.FromImage(bmp);
            Image = bmp;

            var textArea = new Label();
            ConfigureTextArea(textArea, Block.Content);
            DrawTopDownLines(graphics, textArea);
            DrawLeftRightLines(graphics, textArea);
        }

        public override void SetLocation(int x, int y)
        {
            var oldLocation = Location;
            Location = new Point(x, y);

            DownOutput = new Point(
                Location.X + Width / 2,
                Location.Y + Height);

            TopInput = new Point(
                Location.X + Width / 2,
                Location.Y);
        }

        public override void SetLocation(Point newLocation)
        {

            var oldLocation = Location;
            Location = newLocation;

            DownOutput = new Point(
                Location.X + Width / 2,
                Location.Y + Height);

            TopInput = new Point(
                Location.X + Width / 2,
                Location.Y);
        }

        private void DrawTopDownLines(Graphics graphics, Label textArea)
        {
            var from = textArea.Location;
            var to = new Point(from.X + textArea.Width, from.Y);
            graphics.DrawLine(defaultPen, from, to);

            from.Y += textArea.Height;
            to.Y = from.Y;
            graphics.DrawLine(defaultPen, from, to);
        }

        private void DrawLeftRightLines(Graphics graphics, Label textArea)
        {
            var from = textArea.Location;
            var to = new Point(from.X, from.Y + textArea.Height);
            graphics.DrawLine(defaultPen, from, to);

            from.X += textArea.Width;
            to.X = from.X;
            graphics.DrawLine(defaultPen, from, to);
        }
    }
}