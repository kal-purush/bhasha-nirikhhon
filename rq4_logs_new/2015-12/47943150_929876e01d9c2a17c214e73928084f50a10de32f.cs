using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;

namespace WindowsGame1
{
    public abstract class AbstractModel:GameEntity
    {
        public virtual void Draw(GameTime ganeTime, GraphicsDevice graphicsDevice)
        {

        }
    }
}