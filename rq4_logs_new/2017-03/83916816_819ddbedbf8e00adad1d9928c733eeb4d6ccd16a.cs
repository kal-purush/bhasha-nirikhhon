using System.Data.Entity;

namespace DB
{
    public class DBContext : DbContext
    {
        public DBContext()
            :base("DbConnection")
        { }

        public DbSet<Location> Location { get; set; }
        public DbSet<Localization> Localization { get; set; }
        public DbSet<Service> Service { get; set; }
        public DbSet<LocationXLocalization> LocationXLocalization { get; set; }
        public DbSet<MedicalServiceGroups> MedicalServiceGroup { get; set; }
        public DbSet<MedicalServiceGroupsXLocalization> MedicalServiceGroupsXLocalization { get; set; }
        public DbSet<PriceList> PriceList { get; set; }
        public DbSet<ServiceXLocalization> ServiceXLocalization { get; set; }
        public DbSet<ServiceXMedicalServiceGroups> ServiceXMedicalServiceGroup { get; set; }
        public DbSet<ServiceXService> ServiceXService { get; set; }
    }
}