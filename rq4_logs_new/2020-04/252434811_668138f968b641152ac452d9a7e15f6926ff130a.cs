using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HomeWork
{
    class DZ10
    {
        public static void Start()
        {
            Console.WriteLine("Ractangle building\n");

            RectangleBuilding SBuilding1 = new RectangleBuilding(5, 4, 2);
            Console.WriteLine($"Cost of bulding 1 = {SBuilding1.BuildingCost()}");

            RectangleBuilding SBuilding2 = new RectangleBuilding(5, 4, 3);
            Console.WriteLine($"Cost of bulding 2 = {SBuilding2.BuildingCost()}");

            RectangleBuilding SBuilding3 = SBuilding1 + SBuilding2;
            Console.WriteLine($"Cost of bulding 1 + 2 = {SBuilding3.BuildingCost()}");


            Console.WriteLine("\nRound building\n");

            RoundBuilding RBuilding1 = new RoundBuilding(5, 2);
            Console.WriteLine($"Cost of bulding 1 = {RBuilding1.BuildingCost():0.00}");

            RoundBuilding RBuilding2 = new RoundBuilding(5, 3);
            Console.WriteLine($"Cost of bulding 2 = {RBuilding2.BuildingCost():0.00}");

            RoundBuilding RBuilding3 = RBuilding1 + RBuilding2;
            Console.WriteLine($"Cost of bulding 1 + 2 = {RBuilding3.BuildingCost():0.00}");

            Console.ReadLine();
        }
    }

    abstract class Building
    {
        public int FloorCount = 1;

        public Building(int floorCount)
        {
            FloorCount = floorCount;
        }

        public double BuildingCost()
        {
            return Square() * FloorCount;
        }

        public virtual double Square()
        {
            return 0;
        }

    }

    class RectangleBuilding : Building
    {
        public int Width { get; set; }
        public int Length { get; set; }

        public RectangleBuilding(int width, int length, int countFloores)
            : base(countFloores)
        {
            Width = width;
            Length = length;
        }

        public static RectangleBuilding operator +(RectangleBuilding b1, RectangleBuilding b2)
        {
            return new RectangleBuilding(b1.Width, b1.Length + b2.Length, b1.FloorCount);
        }
        public static bool operator >(RectangleBuilding b1, RectangleBuilding b2)
        {
            return b1.BuildingCost() > b2.BuildingCost();
        }
        public static bool operator <(RectangleBuilding b1, RectangleBuilding b2)
        {
           return b1.BuildingCost() < b2.BuildingCost();
        }

        public override double Square()
        {
            return Width * Length;
        }
    }

    class RoundBuilding : Building
    {
        public int Radius { get; set; }

        public static RoundBuilding operator +(RoundBuilding b1, RoundBuilding b2)
        {
            return new RoundBuilding(b1.Radius, b1.FloorCount + b2.FloorCount);
        }
        public static bool operator >(RoundBuilding b1, RoundBuilding b2)
        {
            return b1.BuildingCost() > b2.BuildingCost();
        }
        public static bool operator <(RoundBuilding b1, RoundBuilding b2)
        {
            return b1.BuildingCost() < b2.BuildingCost();
        }

        public RoundBuilding(int radius, int floorCount) 
            : base(floorCount)
        {
            Radius = radius;
        }

        public override double Square()
        {
            return Math.Pow((System.Math.PI * Radius), 2);
        }
    }
}