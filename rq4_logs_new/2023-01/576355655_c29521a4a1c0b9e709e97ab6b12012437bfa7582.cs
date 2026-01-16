using System.Drawing;

namespace Boxinator_V2 {
    public class PercentageRectangle {
        public float X { get; set; }
        public float Y { get; set; }
        public float Width { get; set; }
        public float Height { get; set; }
        public Size ImageSize {get;set;}
        
        public static readonly PercentageRectangle Empty = new PercentageRectangle(0,0,0,0);
        
        public PercentageRectangle(float x, float y, float width, float height) {
            X = x;
            Y = y;
            Width = width;
            Height = height;
        }

        public Rectangle GetRectangle(Size imageSize) {
            return new Rectangle((int)(X * imageSize.Width), (int)(Y * imageSize.Height),
                (int)(Width * imageSize.Width), (int)(Height * imageSize.Height));
        }
        
        public Rectangle ToRectangle(Size imageSize)
        {
            return new Rectangle((int)(X * imageSize.Width), (int)(Y * imageSize.Height), (int)(Width * imageSize.Width), (int)(Height * imageSize.Height));
        }
        
    }
}