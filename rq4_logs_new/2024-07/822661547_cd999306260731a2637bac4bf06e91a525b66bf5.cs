using Microsoft.EntityFrameworkCore;
using ShortSharing.DAL.Entities;

namespace ShortSharing.DAL.Context
{
    public class ApplicationDbContext : DbContext
    {
        public ApplicationDbContext(DbContextOptions<ApplicationDbContext> options) : base(options) { }

        public DbSet<CategoryEntity> Categories { get; set; }
        public DbSet<RentEntity> RentDbSet { get; set; }
        public DbSet<ThingEntity> ThingDbSet { get; set; }
        public DbSet<TypeEntity> TypeDbSet { get; set; }
        public DbSet<UserEntity> UserDbSet { get; set; }
    }
}