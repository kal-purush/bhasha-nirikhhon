using System.Collections.Generic;
using MononokeEngine.Graphics.Drawing.Commands;
using MononokeEngine.Scenes;

namespace MononokeEngine.Graphics.Rendering
{
    
    /// <summary>
    /// Renders everything, with no optimization or culling.
    /// </summary>
    internal class BasicRenderer : IRenderer
    {

        private readonly List<GraphicComponent> _graphics;


        internal BasicRenderer()
        {
            _graphics = new List<GraphicComponent>();
        }
        
        
        public IEnumerable<GraphicComponent> GetVisibleGraphics(Camera camera)
        {
            return _graphics;
        }

        public void AddGraphic(GraphicComponent graphic)
        {
            _graphics.Add(graphic);
        }

        public void RemoveGraphic(GraphicComponent graphic)
        {
            _graphics.Remove(graphic);
        }

        public void Update()
        {
            // Do nothing
        }

        
        
        public bool WillDraw(Camera cam, DrawLine line)
        {
            return true;
        }

        public bool WillDraw(Camera cam, DrawSprite sprite)
        {
            return true;
        }

        public bool WillDraw(Camera cam, DrawRect rect)
        {
            return true;
        }

        public bool WillDraw(Camera cam, SetColor rect)
        {
            return true;
        }
    }
}