using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;
using Microsoft.Xna.Framework.Audio;
using Microsoft.Xna.Framework.Media;
using Microsoft.Xna.Framework.Content;

namespace SkoleProjekt_GodtNavn
{
    public class AnimationContainer_Array
    {
        public List<Texture2D> up = new List<Texture2D>();
        public List<Texture2D> down = new List<Texture2D>();
        public List<Texture2D> left = new List<Texture2D>();
        public List<Texture2D> rigth = new List<Texture2D>();
    }
}