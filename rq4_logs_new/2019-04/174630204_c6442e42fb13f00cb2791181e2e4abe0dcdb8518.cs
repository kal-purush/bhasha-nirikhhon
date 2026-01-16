using System;

namespace DEV_5
{
    internal class Bird : IFlyable
    {
        private Point startPoint;
        private Point targetPoint;

        public FlyableObjectType WhoAmI()
        {
            return FlyableObjectType.Bird;
        }

        public void FlyTo(Point NewPoint)
        {
            targetPoint = NewPoint;
        }

        public double GetFlyTime()
        {
            var dist = GetDistance(startPoint, targetPoint);
            var speed = new Random().Next(0, 20);
            return dist / speed;
        }

        public double GetDistance(Point sPoint, Point tPoint)
        {
            var x = tPoint.x - sPoint.x;
            var y = tPoint.y - sPoint.y;
            var z = tPoint.z - sPoint.z;
            return Math.Sqrt(x * x + y * y + z * z);
        }
    }
}