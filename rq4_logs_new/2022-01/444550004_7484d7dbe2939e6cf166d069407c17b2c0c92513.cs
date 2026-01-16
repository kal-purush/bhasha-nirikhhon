using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace nekoT
{
    public class Camera
    {
        public Matrix Transform { get; set; }
        public Viewport View { get; set; }
        public float RotationDegrees = 0;

        public float Zoom = 1f;

        private float Zoom_ { get { return Zoom = Zoom >= 0.1f ? Zoom : Zoom = 0.1f; } }


        public void Follow(SpriteAtlas target)
        {
            RotationDegrees = RotationDegrees >= 360 ? 0 : RotationDegrees;
            Transform = Matrix.CreateTranslation(-target.Position.X, -target.Position.Y, 0)
            * Matrix.CreateScale(Zoom_, Zoom_, 1)
            * Matrix.CreateRotationZ(RotationDegrees)
            * Matrix.CreateTranslation(View.Width * 0.5f, View.Height * 0.5f, 0);
        }
    }
}