using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Content;
using Microsoft.Xna.Framework.Graphics;

namespace PixelHunter1995.SceneLib
{
    class Dog
    {
        public int X { get; private set; }
        public int Y { get; private set; }
        public int Width { get; private set; }
        public int Height { get; private set; }
        private int gid;

        public Dog(int x, int y, int width, int height, int gid)
        {
            this.X = x;
            this.Y = y;
            this.Width = width;
            this.Height = height;
            this.gid = gid;
        }
        public void Draw(SpriteBatch spriteBatch, Tileset tileset)
        {
            Rectangle destinationRectangle = new Rectangle(X, Y, Width, Height);
            tileset.DrawTile(spriteBatch, destinationRectangle, gid);
        }
    }
}