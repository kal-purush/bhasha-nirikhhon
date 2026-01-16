using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;

namespace PrettyLinesLib
{
    public abstract class Base2DLine
    {
        protected BasicEffect effect;
        protected GraphicsDevice device;
        protected VertexBuffer buffer;

        public virtual Vector2 Start { get; set; }
        public virtual Vector2 End { get; set; }
        public virtual Color Color { get; set; }
        public string Label { get; set; }

        public abstract void Draw(Matrix transformationMatrix);

        public void DrawLabel(SpriteFont font, Color color, SpriteBatch batch = null, Matrix? transformation = null)
        {
            var closebatch = false;
            if (batch == null)
            {
                batch = new SpriteBatch(device);
                closebatch = true;
                if (transformation != null)
                {
                    batch.Begin(SpriteSortMode.Deferred, null, null, null, null, null, (Matrix)transformation);
                }
                else
                {
                    batch.Begin();
                }
            }

            batch.DrawString(font, Label, (Start + End) * 0.5f, color);

            if (closebatch)
            {
                batch.End();
            }
        }
    }
}