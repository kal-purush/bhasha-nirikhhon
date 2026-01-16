using System;
using Microsoft.EntityFrameworkCore;
using BD_.Models;
namespace BD_
{
    //https://metanit.com/sharp/efcore/1.2.php

    public class ApplicationContext : DbContext
    {
        public DbSet<DoctorM> Doctors { get; set; }
        public DbSet<ReceptionM> Receptions { get; set; }
        public DbSet<SheduleM> Shedules { get; set; }
        public DbSet<SpecializationM> Specializations { get; set; }
        public DbSet<UserM> Users { get; set; }

        //https://stackoverflow.com/questions/69961449/net6-and-datetime-problem-cannot-write-datetime-with-kind-utc-to-postgresql-ty

        public ApplicationContext(DbContextOptions options) : base(options)
        {
            //Чтобы работал PlaygroundMethod3 в BD_Tests -> BD_Playground.cs
            AppContext.SetSwitch("Npgsql.EnableLegacyTimestampBehavior", true);
            AppContext.SetSwitch("Npgsql.DisableDateTimeInfinityConversions", true);
        }
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);
            modelBuilder.Entity<UserM>().HasIndex(model => model.Username).IsUnique();
            modelBuilder.Entity<DoctorM>().HasIndex(model => model.FullName).IsUnique();
        }
    }

}