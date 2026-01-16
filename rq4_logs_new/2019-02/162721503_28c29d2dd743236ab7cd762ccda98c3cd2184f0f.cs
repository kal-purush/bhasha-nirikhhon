using RJL.UIP.HW7.LoggerLibrary.Services;
using RJL.UIP.HW7.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RJL.UIP.HW7.Services
{
  public static  class PointsHandler
    {
        public static void PrintPoints(List<Point> points) {
                Console.Clear();
                foreach (var point in points)
                {
                   point.PrintPoint();
                }
         }

        public static List<Point> CreatePointsFromInput(Logger logger) {
            List<Point> Points = new List<Point>();
            logger.Info("Entering count of points");
            Console.WriteLine("--Enter count of points");
            int countPoints = UserСonsoleInteractor.GetPositiveIntValueFromConsoleInput(logger);
            for (int i = 0; i < countPoints; i++)
            {
                int x = GetPointCoordinateFromInput(i,"x",logger);
                int y = GetPointCoordinateFromInput(i, "y", logger);
                Point point = new Point(x, y);
                Points.Add(point);
                logger.Info($"There was created point with coordinates ({point.X},{point.Y})");
            }
            return Points;
        }
        public static int GetPointCoordinateFromInput(int pointCount,string pointType, Logger logger) {
            logger.Info($"Entering coordinate {pointType} for point number {pointCount + 1}");
            Console.WriteLine($"--Enter coordinate {pointType} for point number {pointCount + 1}");
            return  UserСonsoleInteractor.GetPositiveIntValueFromConsoleInput(logger);
        }
    }
}