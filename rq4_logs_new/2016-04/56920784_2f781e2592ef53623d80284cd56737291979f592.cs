using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OsmTest.Core.Models
{
   public class Crs
   {
      public int Type { get; set; }
   }

   public class Point
   {
      public List<double> coordinates { get; set; }
      public List<int> bbox { get; set; }
      public Crs crs { get; set; }
      public int type { get; set; }
   }

   public class Coordinates
   {
      public int CrowdsourcedPlaceType { get; set; }
      public string CreatorId { get; set; }
      public string PlaceId { get; set; }
      public Point Point { get; set; }
      public string Description { get; set; }
      public string CreationTime { get; set; }
   }
}