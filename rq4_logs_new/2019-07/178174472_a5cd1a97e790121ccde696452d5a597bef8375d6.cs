using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using MononokeEngine.Utils;

namespace MononokeEngine.Graphics.Drawing.Commands
{
    internal class DrawRect : IDrawCommand
    {
        private readonly Rectangle _rect;
        private readonly Color _color;

        public DrawRect(Rectangle rect, Color color)
        {
            _rect = rect;
            _color = color;
        }
        
        
        public void Execute()
        {
            Texture px = Mononoke.Graphics.Pixel;
            Mononoke.Graphics.SpriteBatch.Draw(px.Texture2D, _rect, px.ClipRect, _color);
        }
    }
}