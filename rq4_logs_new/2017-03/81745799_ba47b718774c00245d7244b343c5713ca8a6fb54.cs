using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using static PlaneLog.Models.Flight;
using static PlaneLog.Models.Plane;

namespace PlaneLog
{
    public class Squawks
    {
        public int Id { get; }
        public int PlaneId { get; set; }
        public char Issue { get; set; }
        public DateTime DateRercorded { get; set; }
        public bool Resolved { get; set; }
        public DateTime DateResolved { get; set; }
        public char WhoResolved { get; set; }
        public decimal CostToResolve { get; set; }
        public char ContactPhone { get; set; }
        public char ContactName { get; set; }


    }
}