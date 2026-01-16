using Microsoft.EntityFrameworkCore;
using TestDB.Models;

namespace TestDB.Data
{
    public class ProductContext : DbContext
    {
        public ProductContext()
        {
        }
        public ProductContext(DbContextOptions<ProductContext> options) : base(options) { }

        public DbSet<Category> Category { get; set; }
        public DbSet<Product> Product { get; set; }
        public DbSet<CategoriesProducts> CategoriesProducts { get; set; }


        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<CategoriesProducts>(entity =>
            {
                entity.HasKey(sc => new { sc.CategoryId, sc.ProductId });
                entity.Property(e => e.CategoryId).HasColumnName("CategoryID");
                entity.Property(e => e.ProductId).HasColumnName("ProductID");

                entity.HasOne(sc => sc.Category)
                    .WithMany(p => p.Products)
                    .HasForeignKey(sc => sc.CategoryId)
                    .OnDelete(DeleteBehavior.NoAction)
                    .HasConstraintName("FK_Product_Category");
            });

            base.OnModelCreating(modelBuilder);
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                optionsBuilder.UseSqlServer("Server=.;Database=Test;Integrated Security=True;");
            }

            base.OnConfiguring(optionsBuilder);
        }
    }
}