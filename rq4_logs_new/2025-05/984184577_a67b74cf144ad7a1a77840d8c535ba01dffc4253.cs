using AnimalShelter.DataAccess.Configurations;
using AnimalShelter.DataAccess.Entities;
using Microsoft.EntityFrameworkCore;

namespace AnimalShelter.DataAccess
{
    public class AnimalShelterDbContext : DbContext
    {
        public AnimalShelterDbContext(DbContextOptions<AnimalShelterDbContext> options) : base(options)
        {
        }

        public DbSet<AnimalEntity> Animal { get; set; }

        public DbSet<TypeAnimalEntity> TypeAnimal { get; set; }

        public DbSet<AnimalStatusEntity> AnimalStatus { get; set; }

        public DbSet<UserEntity> User { get; set; }

        public DbSet<RoleEntity> Role { get; set; }

        public DbSet<AdoptionEntity> Adoption { get; set; }

        public DbSet<AdoptionApplicationEntity> AdoptionApplication { get; set; }

        public DbSet<StatusAdoptionEntity> StatusAdoption { get; set; }

        public DbSet<EmployeeEntity> Employee { get; set; }

        public DbSet<TemporaryAccommodationEntity> TemporaryAccommodation { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ApplyConfiguration(new AnimalConfiguration());
            modelBuilder.ApplyConfiguration(new TypeAnimalConfiguration());
            modelBuilder.ApplyConfiguration(new AnimalStatusConfiguration());
            modelBuilder.ApplyConfiguration(new UserConfiguration());
            modelBuilder.ApplyConfiguration(new RoleConfiguration());
            modelBuilder.ApplyConfiguration(new AdoptionConfiguration());
            modelBuilder.ApplyConfiguration(new AdoptionApplicationConfiguration());
            modelBuilder.ApplyConfiguration(new StatusAdoptionConfiguration());
            modelBuilder.ApplyConfiguration(new EmployeeConfiguration());
            modelBuilder.ApplyConfiguration(new TemporaryAccommodationConfiguration());
        }
    }
}