using System;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using MononokeEngine.Graphics.Core.Rendering;
using MononokeEngine.Scenes;

namespace MononokeEngine.Graphics.Drawing.Commands
{
    internal class DrawText : AbstractDrawCommand
    {

        public SpriteFont Font { get; }
        public Vector2 Position { get; }
        
        public string Text { get; }
        
        private readonly Color _color;
        

        public DrawText(SpriteFont font, string text, Vector2 position, Color color)
        {
            Font = font;
            Text = text;
            Position = position;
            _color = color;
        }
        
        
        public override void Execute(Camera cam)
        {
            Vector2 position = cam.GetRenderPosition(Position);
                          
            Mononoke.Graphics.SpriteBatch.DrawString(
                    Font,
                    Text,
                    Utils.Math.Floor(position),
                    _color
                );
        }

        public override bool Accept(Camera cam, IRenderer renderer)
        {
            // TODO
            return true;
            //return renderer.WillDraw(cam, this);
        }
    }
}