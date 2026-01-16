using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Entity;
using System.Data.Entity.ModelConfiguration.Conventions;
using Collector2.Model;

namespace Collector2.DataContext
{
    public class CollectorContext : DbContext
    {
        public DbSet<Item> Item { get; set; }
        public DbSet<Owner> Owner { get; set; }
        public DbSet<Software> Software { get; set; }
        public DbSet<Hardware> Hardware { get; set; }
        public DbSet<HardwareSpec> HardwareSpec { get; set; }
        public DbSet<Format> Format { get; set; }
        public DbSet<Category> Category { get; set; }
        public DbSet<Review> Review { get; set; }
        public DbSet<ScoreSystem> ScoreSystem { get; set; }

        public CollectorContext() : base("CollectorDB")
        {
            Configuration.ProxyCreationEnabled = false;
        }

        protected override void OnModelCreating(DbModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            //Don't want pluralized table names
            modelBuilder.Conventions.Remove<PluralizingTableNameConvention>();
        }
    }
}