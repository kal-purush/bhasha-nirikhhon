using System.Windows;
using GameOne.Source.Renderer;

namespace GameOne.Source.Factories
{
    using System;
    using Entities;

    public class ProjectileFactory
    {
        public static Projectile MakeProjectile(Character source, Enum bulletType)
        {
            double direction = source.Direction;
            double velocityX = 0;
            double velocityY = 0;

            if (direction == 1.5 * Math.PI)
            {
                velocityY = -6;
            }
            else if (direction == 0.5 * Math.PI)
            {
                velocityY = 6;
            }
            else if (direction == Math.PI)
            {
                velocityX = -6;
            }
            else if (direction == 0)
            {
                velocityX = 6;
            }

            Vector velocity = new Vector(velocityX, velocityY);

            return new Projectile(source.Position.X, source.Position.Y, source.Direction, 0.1, new Spritesheet(), source, velocity); //Check the radius
        }
    }
}