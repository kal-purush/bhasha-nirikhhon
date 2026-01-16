using System;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace HumansInHarmony.Models
{
    public partial class HarmonyContext : DbContext
    {
        public HarmonyContext()
        {
        }

        public HarmonyContext(DbContextOptions<HarmonyContext> options)
            : base(options)
        {
        }

        public virtual DbSet<DislikedSongs> DislikedSongs { get; set; }
        public virtual DbSet<LikedSongs> LikedSongs { get; set; }
        public virtual DbSet<SongInfo> SongInfo { get; set; }
        public virtual DbSet<User> User { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
#warning To protect potentially sensitive information in your connection string, you should move it out of source code. See http://go.microsoft.com/fwlink/?LinkId=723263 for guidance on storing connection strings.
                optionsBuilder.UseSqlServer("Data Source=humansinharmony.database.windows.net;Database=Harmony;User=djk@humansinharmony.database.windows.net;Password=40Pearl!;Trusted_Connection=True;Encrypt=True; Integrated Security=False");
            }
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.HasAnnotation("ProductVersion", "2.2.4-servicing-10062");

            modelBuilder.Entity<DislikedSongs>(entity =>
            {
                entity.HasIndex(e => e.UserId);

                entity.HasOne(d => d.User)
                    .WithMany(p => p.DislikedSongs)
                    .HasForeignKey(d => d.UserId);
            });

            modelBuilder.Entity<LikedSongs>(entity =>
            {
                entity.HasIndex(e => e.UserId);

                entity.HasOne(d => d.User)
                    .WithMany(p => p.LikedSongs)
                    .HasForeignKey(d => d.UserId);
            });
        }
    }
}