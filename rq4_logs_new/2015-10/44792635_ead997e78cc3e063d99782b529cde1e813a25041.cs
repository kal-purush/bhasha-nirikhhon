using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Audio;
using Microsoft.Xna.Framework.Content;
using Microsoft.Xna.Framework.GamerServices;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;
using Microsoft.Xna.Framework.Media;

namespace Assignment3.Entities
{
    public abstract class Entity
    {
        public Entity()
        {

        }
        
        public abstract void update(KeyboardState keyboard, GamePadState gamepad);

        public abstract void draw(SpriteBatch sb);
    }
}