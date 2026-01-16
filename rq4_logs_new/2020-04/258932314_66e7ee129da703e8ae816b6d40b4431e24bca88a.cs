using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.EntityFrameworkCore;
using SPS.AEM.Database.Entities;

namespace SPS.AEM.Database.Contexts
{
    public class AemDatabaseContext : DbContext
    {
        public AemDatabaseContext(DbContextOptions options)
        : base(options)
        {
        }

        public DbSet<Login> Logins { get; set; }

        public DbSet<User> Users { get; set; }

        public DbSet<Customer> Customers { get; set; }

        public DbSet<LoadType> LoadTypes { get; set; }

        public DbSet<Role> Roles { get; set; }

        public DbSet<District> Districts { get; set; }

        public DbSet<Taluka> Talukas { get; set; }

        public DbSet<Village> Villages { get; set; }

        public DbSet<Station> Stations { get; set; }

        public DbSet<Section> Sections { get; set; }

        public DbSet<Feeder> Feeders { get; set; }

        public DbSet<Transformer> Transformers { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);
            modelBuilder.Entity<Login>().ToTable(nameof(Login)).HasNoKey();

            modelBuilder.Entity<User>().ToTable(nameof(User)).HasKey(p => p.Id);
            modelBuilder.Entity<User>().Property(p => p.Id).UseIdentityColumn();

            modelBuilder.Entity<Customer>().ToTable(nameof(Customer)).HasKey(p => p.Id);
            modelBuilder.Entity<Customer>().Property(p => p.Id).UseIdentityColumn();

            modelBuilder.Entity<LoadType>().ToTable(nameof(LoadType)).HasKey(p => p.Id);
            modelBuilder.Entity<LoadType>().Property(p => p.Id).UseIdentityColumn();

            modelBuilder.Entity<Role>().ToTable(nameof(Role)).HasKey(p => p.Id);
            modelBuilder.Entity<Role>().Property(p => p.Id).UseIdentityColumn();

            modelBuilder.Entity<District>().ToTable(nameof(District)).HasKey(p => p.Id);
            modelBuilder.Entity<District>().Property(p => p.Id).UseIdentityColumn();
            
            modelBuilder.Entity<Taluka>().ToTable(nameof(Taluka)).HasKey(p => p.Id);
            modelBuilder.Entity<Taluka>().HasOne(p => p.District).WithMany(p => p.Talukas);
            modelBuilder.Entity<Taluka>().Property(p => p.Id).UseIdentityColumn();

            modelBuilder.Entity<Village>().ToTable(nameof(Village)).HasKey(p => p.Id);
            modelBuilder.Entity<Village>().HasOne(p => p.Taluka).WithMany(p => p.Villages);
            modelBuilder.Entity<Village>().Property(p => p.Id).UseIdentityColumn();

            modelBuilder.Entity<Station>().ToTable(nameof(Station)).HasKey(p => p.Id);
            modelBuilder.Entity<Station>().HasOne(p => p.Taluka).WithMany(p => p.Stations)
                .OnDelete(DeleteBehavior.ClientSetNull);
            modelBuilder.Entity<Station>().HasOne(p => p.District).WithMany(p => p.Stations)
                .OnDelete(DeleteBehavior.ClientSetNull);
            modelBuilder.Entity<Station>().HasOne(p => p.Village).WithMany(p => p.Stations)
                .OnDelete(DeleteBehavior.ClientSetNull);
            modelBuilder.Entity<Station>().Property(p => p.Id).UseIdentityColumn();

            modelBuilder.Entity<Section>().ToTable(nameof(Section)).HasKey(p => p.Id);
            modelBuilder.Entity<Section>().HasOne(p => p.Station).WithMany(p =>p.Sections)
                .OnDelete(DeleteBehavior.ClientSetNull);
            modelBuilder.Entity<Section>().Property(p => p.Id).UseIdentityColumn();

            modelBuilder.Entity<Feeder>().ToTable(nameof(Feeder)).HasKey(p => p.Id);
            modelBuilder.Entity<Feeder>().HasOne(p => p.Section).WithMany(p => p.Feeders)
                .OnDelete(DeleteBehavior.ClientSetNull);
            modelBuilder.Entity<Feeder>().Property(p => p.Id).UseIdentityColumn();

            modelBuilder.Entity<Transformer>().ToTable(nameof(Transformer)).HasKey(p => p.Id);
            modelBuilder.Entity<Transformer>().HasOne(p => p.Feeder).WithMany(p => p.Transformers)
                .OnDelete(DeleteBehavior.ClientSetNull);
            modelBuilder.Entity<Transformer>().Property(p => p.Id).UseIdentityColumn();
        }
    }
}