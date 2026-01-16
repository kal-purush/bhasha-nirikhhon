using RentForPark.Models;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.ModelConfiguration.Conventions;
using System.Linq;
using System.Web;

namespace RentForPark.DAL
{
    public class RentContext : DbContext
    {
        public RentContext() : base("RentContext")
        {
        }


        public DbSet<User> User { get; set; }
        public DbSet<Comenzi> Comenzi { get; set; }
        public DbSet<Produse> Produse { get; set; }
        protected override void OnModelCreating(DbModelBuilder modelBuilder)
        {

            modelBuilder.Conventions.Remove<PluralizingTableNameConvention>();
        }
    }
}