namespace jobsearch.Data
{
    public class DataContext : DbContext
    {
        public DataContext(DbContextOptions options) : base(options)
        {
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseNpgsql("host=db;user id=jobsearch;password=jobsearch;database=jobsearch_dev;Pooling=false;Timeout=300;CommandTimeout=300;");

#if DEBUG
            optionsBuilder.EnableSensitiveDataLogging();
#endif
        }
    }
}