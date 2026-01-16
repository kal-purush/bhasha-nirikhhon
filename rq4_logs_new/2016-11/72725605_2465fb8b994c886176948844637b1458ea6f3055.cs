using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using SFML;
using SFML.System;
using SFML.Window;
using SFML.Graphics;
using SFML.Audio;

namespace SFML_Prog2_Gruppe1
{
    public class CollisionUtil
    {
        public static Vector2f CalculateCollisionDepth(FloatRect characterBounds, FloatRect otherBounds)
        {
            //If the distance on both axis is higher than the sum of the half of both Rectangles, they are not colliding
            if (!characterBounds.Intersects(otherBounds))
            {
                //Not colliding at all, Zero Vector is returned
                return new Vector2f(0, 0);
            }

            //Create depth Vector
            Vector2f depth = new Vector2f(0, 0);

            //Get the Bounds Center
            Vector2f characterBoundsCenter = new Vector2f(characterBounds.Left + characterBounds.Width / 2, characterBounds.Top + characterBounds.Height / 2);
            Vector2f otherBoundsCenter = new Vector2f(otherBounds.Left + otherBounds.Width / 2, otherBounds.Top + otherBounds.Height / 2);

            //Get the distance between both Rectangles on both axis
            float distanceX = characterBoundsCenter.X - otherBoundsCenter.X;
            float distanceY = characterBoundsCenter.Y - otherBoundsCenter.Y;

            //Is the distance on X-Axis higher than 0 -> Determine relative position of other Rectangle
            if (distanceX > 0)
            {
                //Depth on X-Axis is half of both Rectangles - the distance
                depth.X = (characterBounds.Width / 2 + otherBounds.Width / 2) - distanceX;
            }
            //Is the distance on X-Axis lower than 0: turn around the + and -
            else depth.X = -(characterBounds.Width / 2 + otherBounds.Width / 2) - distanceX;

            //Same on the Y-Axis
            if (distanceY > 0) depth.Y = (characterBounds.Height / 2 + otherBounds.Height / 2) - distanceY;
            else depth.Y = -(characterBounds.Height / 2 + otherBounds.Height / 2) - distanceY;

            //Return the final depth
            return depth;

        }



    }
}