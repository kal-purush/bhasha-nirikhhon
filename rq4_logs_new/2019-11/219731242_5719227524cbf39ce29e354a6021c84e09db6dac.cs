using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xna.Framework;

namespace SkoleProjekt_GodtNavn
{
    public class Camera
    {
        public Matrix Transform { get; private set; }

        public void Follow(GameObject target)
        {
            var position = Matrix.CreateTranslation(
               -target.transform.position.X - (target.CollisionBox.Width / 2),
               -target.transform.position.Y - (target.CollisionBox.Height / 2),
               0);

            var offset = Matrix.CreateTranslation(
                Gameworld.ScreenSize.X / 2,
                Gameworld.ScreenSize.Y / 2,
                0);

            Transform = position * offset;
        }
    }
}