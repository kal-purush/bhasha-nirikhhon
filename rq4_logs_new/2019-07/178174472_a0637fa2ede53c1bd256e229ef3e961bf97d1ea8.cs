using MononokeEngine.Physics.Colliders;

namespace MononokeEngine.Physics
{
    internal static class CollisionChecks
    {
        
        public static bool PlaceMeeting(dynamic first, dynamic second)
        {
            return PlaceMeetingImpl(first, second) || PlaceMeetingImpl(first, second);
        }
        
        
        
        
        // Default: return false
        
        private static bool PlaceMeetingImpl(object first, object second)
        {
            return false;
        }
        
        
        
        
        // BOX vs BOX
        
        private static bool PlaceMeetingImpl(BoxCollider first, BoxCollider second)
        {
            if (first.Right <= second.Left) return false;
            if (first.Bottom <= second.Top) return false;
            if (first.Left >= second.Right) return false;
            if (first.Top >= second.Bottom) return false;
            return true;
        }
        
        
        
        
        /*
        private static bool PlaceMeetingImpl(BoxCollider box, CircleCollider circle)
        {
            //check is c center point is in the rect
            if (Util.InRect(circle.CenterX, circle.CenterY, box.Left, box.Top, box.Width, box.Height)) {
                return true;
            }

            //check to see if any corners are in the circle
            if (Util.DistanceRectPoint(circle.CenterX, circle.CenterY, box.Left, box.Top, box.Width, box.Height) < circle.Radius) {
                return true;
            }

            //check to see if any lines on the box intersect the circle
            Line2 boxLine;

            boxLine = new Line2(box.Left, box.Top, box.Right, box.Top);
            if (boxLine.IntersectCircle(new Vector2(circle.CenterX, circle.CenterY), circle.Radius)) return true;

            boxLine = new Line2(box.Right, box.Top, box.Right, box.Bottom);
            if (boxLine.IntersectCircle(new Vector2(circle.CenterX, circle.CenterY), circle.Radius)) return true;

            boxLine = new Line2(box.Right, box.Bottom, box.Left, box.Bottom);
            if (boxLine.IntersectCircle(new Vector2(circle.CenterX, circle.CenterY), circle.Radius)) return true;

            boxLine = new Line2(box.Left, box.Bottom, box.Left, box.Top);
            if (boxLine.IntersectCircle(new Vector2(circle.CenterX, circle.CenterY), circle.Radius)) return true;

            return false;
        }
        */
    }
}