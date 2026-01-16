using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace NbodyWithUI.Models
{
    internal class Particle
    {
        public double Mass { get; set; }
        public (double x, double y) Position { get; set; }
        public List<(double x, double y)> Last100Coordinates { get; set; }
        public uint ParticleCoordListSizeLimit { get; set; } = 100;

        public void addCoordinate((double a, double b) coordXY)
        {
            Last100Coordinates.Add(coordXY);
            if(Last100Coordinates.Count > ParticleCoordListSizeLimit)
                Last100Coordinates.RemoveAt(Last100Coordinates.Count - 1);
        }
        public (double x, double y, double c) currentVelocityXYC(int timestep)
        {
            var lastIndex = Last100Coordinates.Count-1;

            var dx = Last100Coordinates[lastIndex].x - Last100Coordinates[lastIndex-1].x;
            var dy = Last100Coordinates[lastIndex].y - Last100Coordinates[lastIndex-1].y;

            var absC = Math.Sqrt(dx * dx + dy * dy);

            return (dx/timestep, dy/timestep, absC/timestep);
        }

        public double currentAngleInDegrees()
        {
            var lastIndex = Last100Coordinates.Count - 1;

            var dx = Last100Coordinates[lastIndex].x - Last100Coordinates[lastIndex - 1].x;
            var dy = Last100Coordinates[lastIndex].y - Last100Coordinates[lastIndex - 1].y;

            return Math.Atan2(dx, dy);

        }
        public Particle()
        {

        }
    }
}