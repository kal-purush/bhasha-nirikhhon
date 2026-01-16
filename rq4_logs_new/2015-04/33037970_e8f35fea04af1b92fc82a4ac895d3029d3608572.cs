using Microsoft.Xna.Framework.Graphics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Platformer
{
    class Door : Invertible
    {
        public Door(int x, int y, int width, int height, Texture2D image, Texture2D image_i)
        {
            this.spriteX = x;
            this.spriteY = y;
            this.spriteWidth = width;
            this.spriteHeight = height;

            this.image = image;
            this.image_i = image_i;
        }

        public int getX()
        {
            return spriteX;
        }
        public int getY()
        {
            return spriteY;
        }
        public int getWidth()
        {
            return spriteWidth;
        }
        public int getHeight()
        {
            return spriteHeight;
        }
    }
}