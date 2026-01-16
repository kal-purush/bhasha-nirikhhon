using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Identity.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using System.Reflection.Emit;
using WineShopWebAPI.Authentication;
using WineShopWebAPI.Models;

namespace WineShopWebAPI.Authentication
{


    public class ApplicationDbContext : IdentityDbContext<ApplicationUser>
    {
        public ApplicationDbContext(DbContextOptions<ApplicationDbContext> options) : base(options)
        {

        }
        protected override void OnModelCreating(ModelBuilder builder)
        {
            base.OnModelCreating(builder);

            builder.Entity<Shop>().HasData(
           new Shop { Id= 1, Name = "Poojitha Wines" , Address="Hebbal, Bangalore",Email="pw@g.com",PhoneNo="123456"},
           new Shop { Id = 2,  Name = "Ranjith Bar", Address = "Kodigehalli, Bangalore", Email = "pw@g.com", PhoneNo = "123456" }
       // Add more entities as needed
       );
        }
        public DbSet<Shop> Shops { get; set; }
        public DbSet<AccountGroup> AccountGroups { get; set; }
        public DbSet<ChartOfAccount> ChartOfAccounts { get; set; }
        public DbSet<Customer> Customers { get; set; }
        public DbSet<ExpenseClaim> ExpenseClaims { get; set; }
        public DbSet<Item> Items { get; set; }
        public DbSet<ItemStock> ItemStocks { get; set; }
        public DbSet<PurchaseRegister> PurchaseRegisters { get; set; }
        public DbSet<SalesRegister> SalesRegisters { get; set; }
        public DbSet<Supplier> Suppliers { get; set; }
        public DbSet<SupplierItem> SupplierItems { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseSqlServer("Server=tcp:poojithahome.database.windows.net,1433;Initial Catalog=PoojithaHome;Persist Security Info=False;User ID=poojithaadmin;Password=Ranjith@7591;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;");
        }
    }

}


