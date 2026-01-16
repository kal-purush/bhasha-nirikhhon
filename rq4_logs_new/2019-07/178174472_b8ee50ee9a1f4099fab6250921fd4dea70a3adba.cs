using Microsoft.Xna.Framework;
using MononokeEngine.Scenes;

namespace MononokeEngine.Graphics.Components
{
    
    
    /// <summary>
    /// TODO
    /// </summary>
    public class RectRenderer : Graphic
    {
        
        public Rectangle Rect { get; set; }
        
        
        protected override void RenderImpl()
        {
            throw new System.NotImplementedException();
        }


        public override bool IsOnCameraBounds(Camera camera)
        {
            float x = camera.Position.X;
            float y = camera.Position.Y;
        
            if (Position.X > x + camera.Width)
                return false;
        
            if (Position.Y > y + camera.Height)
                return false;
        
            if (Position.X + Rect.Width < x)
                return false;
        
            if (Position.Y + Rect.Height < y)
                return false;

            return true;
        }
    }
}