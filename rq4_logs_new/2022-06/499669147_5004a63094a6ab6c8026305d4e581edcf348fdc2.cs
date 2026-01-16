using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NbodyWithUI.Models
{
    public static class Plotter
    {
        public static bool LimitToWorldBoundries { get; set; } = false;
        public static bool coordsAreValid(List<Particle> particles)
        {
            int reference = particles[0].CoordinateHistory.X.Count();
            foreach (var particle in particles)
            {
                if (particle.CoordinateHistory.X.Count() != reference ||
                   particle.CoordinateHistory.Y.Count() != reference)
                    return false;
            }
            return true;
        }
        public static void plotWorldScottPlot(World world)
        {
            var plt = new ScottPlot.Plot(800, 800);
            int breakPoint = world.Particles[0].CoordinateHistory.X.Count();
            bool valid = false;
            //valid = coordsAreValid(world.Particles);
            bool breakLoop = false;
            if (true)
            {
                //if (LimitToWorldBoundries)
                //{
                //    breakPoint = 0;
                //    foreach (var p in world.Particles)
                //    {
                //        for (int i = 0; i < p.CoordinateHistory.X.Count(); i++)
                //        {
                //            if (p.CoordinateHistory.X[i] > world.Width ||
                //                p.CoordinateHistory.Y[i] > world.Height)
                //            {
                //                breakLoop = true;
                //                break;
                //            }
                //            breakPoint++;
                //            if (breakLoop)
                //                break;
                //        }
                //        if (breakLoop)
                //            break;
                //    }
                //}

                List<double> Xcoords = new List<double>();
                List<double> Ycoords = new List<double>();
                int minLength = Int32.MaxValue;
                foreach (var p in world.Particles)
                {
                    Xcoords = p.CoordinateHistory.X.Where(a => Math.Abs(a) < world.Width).ToList();
                    Ycoords = p.CoordinateHistory.Y.Where(a => Math.Abs(a) < world.Width).ToList();
                    if (minLength > Xcoords.Count)
                        minLength = Xcoords.Count;
                    if (minLength > Ycoords.Count)
                        minLength = Ycoords.Count;
                }

                foreach (var p in world.Particles)
                {
                    Xcoords = p.CoordinateHistory.X.Where(a => Math.Abs(a) < world.Width).Take(minLength).ToList();
                    Ycoords = p.CoordinateHistory.Y.Where(a => Math.Abs(a) < world.Width).Take(minLength).ToList();
                    plt.AddScatter(Xcoords.ToArray(), Ycoords.ToArray());

                }






                Console.WriteLine("Total of data points in simulation: " + breakPoint * world.Particles.Count());
                //if (Xcoords.Count() > 0 && Ycoords.Count() > 0)
                plt.SaveFig("scottplotfig.png");
            }
        }
    }
}
