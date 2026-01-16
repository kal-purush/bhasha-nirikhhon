using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MixTelematics.Models
{
    public class QuadTreeNodeOptimized
    {
        public double X { get; set; }
        public double Y { get; set; }
        public QuadTreeNodeOptimized NW { get; set; }
        public QuadTreeNodeOptimized NE { get; set; }
        public QuadTreeNodeOptimized SW { get; set; }
        public QuadTreeNodeOptimized SE { get; set; }
        public List<VehiclePosition> Vehicles { get; set; }

        public QuadTreeNodeOptimized(double x, double y)
        {
            X = x;
            Y = y;
            Vehicles = new List<VehiclePosition>();
        }
    }
}