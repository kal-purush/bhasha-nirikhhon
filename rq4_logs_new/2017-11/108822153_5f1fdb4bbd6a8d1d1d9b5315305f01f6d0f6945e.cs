using DCode.Common;
using System.Data.Entity;
using System.Data.Entity.Infrastructure;

namespace DCode.Data.DbContexts
{
    public class ReportingDbContext : DbContext
    {
        public ReportingDbContext()
            : base(Constants.DCodeEntities)
        {
            this.Configuration.LazyLoadingEnabled = false;
        }
        protected override void OnModelCreating(DbModelBuilder modelBuilder)
        {
            throw new UnintentionalCodeFirstException();
        }

        public virtual DbSet<daily_usage_statistics> DailyUsageStatistics { get; set; }
    }
}