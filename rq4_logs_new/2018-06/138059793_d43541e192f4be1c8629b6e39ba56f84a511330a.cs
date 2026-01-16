using MySql.Data.Entity;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Traffic.Models
{
    [DbConfigurationType(typeof(MySqlEFConfiguration))]
    class TrafficContext : DbContext
    {
        public DbSet<Point1> points1 { get; set; }
        public DbSet<Point2> points2 { get; set; }
        public DbSet<Segment> segment { get; set; }
        public DbSet<Intersection> intersctions { get; set; }
    }
}