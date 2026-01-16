using System.Windows;

namespace SnapNShare.Extensions
{
    public static class Geometry
    {
        public static void SwapPointsIfRequired(ref Point initial, ref Point current)
        {
            double initialX = initial.X;
            double initialY = initial.Y;
            double currentX = current.X;
            double currentY = current.Y;

            if (initialX > currentX)
            {
                currentX = initial.X;
                initialX = current.X;
            }
            if (initialY > currentY)
            {
                currentY = initial.Y;
                initialY = current.Y;
            }

            initial = new Point(initialX, initialY);
            current = new Point(currentX, currentY);
        }
    }
}