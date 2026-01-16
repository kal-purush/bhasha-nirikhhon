using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;

namespace AIRogue.Engine
{
    class ControlledCamera : Camera2D
    {
        public ControlledCamera(Viewport viewport) : base(viewport)
        {
            InputHandler.KeyDownListeners.Add(OnKeyDown);
        }

        public void OnKeyDown(Keys key)
        {
            if (key == Keys.OemPlus)
                this.Zoom *= .9f;
            else if (key == Keys.OemMinus)
            {
                this.Zoom *= 1.1f;
            } else if (key == Keys.Back)
            {
                this.Zoom = 1f;
            }
        }
    }
}