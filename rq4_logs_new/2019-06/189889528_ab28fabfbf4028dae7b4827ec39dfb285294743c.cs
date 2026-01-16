using System;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace BethanysPieShop.Web.EFModels
{
    public partial class bethanypiesContext : DbContext
    {
        public bethanypiesContext()
        {
        }

        public bethanypiesContext(DbContextOptions<bethanypiesContext> options)
            : base(options)
        {
        }

        public virtual DbSet<Pie> Pie { get; set; }

//        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
//        {
//            if (!optionsBuilder.IsConfigured)
//            {
//#warning To protect potentially sensitive information in your connection string, you should move it out of source code. See http://go.microsoft.com/fwlink/?LinkId=723263 for guidance on storing connection strings.
//                optionsBuilder.UseNpgsql("User ID=postgres;Password=taiotoshi2!;Host=localhost;Port=5432;Database=bethanypies;Pooling=true;");
//            }
//        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.HasAnnotation("ProductVersion", "2.2.4-servicing-10062");

            modelBuilder.Entity<Pie>(entity =>
            {
                entity.ToTable("pie");

                entity.Property(e => e.Id).HasColumnName("id");

                entity.Property(e => e.ImageThumbnailUrl)
                    .IsRequired()
                    .HasMaxLength(150);

                entity.Property(e => e.ImageUrl)
                    .IsRequired()
                    .HasMaxLength(150);

                entity.Property(e => e.LongDescription)
                    .IsRequired()
                    .HasMaxLength(900);

                entity.Property(e => e.Name)
                    .IsRequired()
                    .HasColumnName("name")
                    .HasMaxLength(100);

                entity.Property(e => e.Price)
                    .HasColumnName("price")
                    .HasColumnType("numeric(19,2)");

                entity.Property(e => e.ShortDescription)
                    .IsRequired()
                    .HasMaxLength(200);
            });
        }
    }
}