using BeautyShop.Model.Models;
using System.Data.Entity;

namespace BeautyShop.Data
{
    public class BeautyShopDbContext : DbContext
    {
        public BeautyShopDbContext() : base("BeautyShopConnection") //Truyền vào 1 cái Connection String
        {
            this.Configuration.LazyLoadingEnabled = false; //Tìm hiểu google
        }
        public DbSet<Footer> Footers { set; get; }
        public DbSet<Menu> Menus { set; get; }
        public DbSet<MenuGroup> MenuGroups { set; get; }
        public DbSet<Order> Orders { set; get; }
        public DbSet<OrderDetail> OrderDetails { set; get; }
        public DbSet<Page> Pages { set; get; }

        public DbSet<PostCategory> PostCategorys { set; get; }

        public DbSet<PostTag> PostTags { set; get; }

        public DbSet<Product> Products { set; get; }

        public DbSet<ProductCategory> ProductCategorys { set; get; }
        public DbSet<ProductTag> ProductTags { set; get; }
        public DbSet<Slide> Slides { set; get; }
        public DbSet<SupportOnline> SupportOnlines { set; get; }
        public DbSet<SystemConfig> SystemConfigs { set; get; }
        public DbSet<Tag> Tags { set; get; }
        public DbSet<VisitorStatistic> VisitorStatistics { set; get; }


        //Ghi de phuong thuc OnModelCreating se chay khi khoi tao emtity farmwork
        protected override void OnModelCreating(DbModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);
        }

    }
}