using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Traffic.Models
{
    class TrafficBoard
    {
        public int id { get; set; }
        public virtual Point1 point1 { get; set; }
        public virtual Point2 point2 { get; set; }
        public int boardNumber { get; set; }
        public virtual Street street { get; set; }
    }
}