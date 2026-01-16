using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Configuration;
using ContactSvc.Dtos;
using ContactSvc.Settings;
using Microsoft.Extensions.Logging;

namespace ContactSvc.Data
{
    public class ContactDbContext : DbContext
    {
        private readonly IConfiguration _configuration;
        private readonly IAzureSecrets _azureSecrets;
        private readonly ILogger _logger;
       private string _connectionString = string.Empty;

        public ContactDbContext(DbContextOptions<ContactDbContext> options, 
            IConfiguration configuration,
            IAzureSecrets azureSecrets,
            ILogger logger) : base(options)
        {
            _configuration = configuration;
            _azureSecrets = azureSecrets;
            _logger = logger;
        }

        public DbSet<CustomerMessage> CustomerMessages { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ApplyConfigurationsFromAssembly(typeof(ContactDbContext).Assembly);
        }

        public string CreateConnectionString()
        {
            string? conSetting =_configuration.GetConnectionString("DefaultConnection");
            if (conSetting != null)
            {
                DBSettings dbSettings = _azureSecrets.GetDBSettingsAsync(_logger, _configuration).Result;

                _connectionString = string.Format(conSetting, 
                    dbSettings.ServerName, dbSettings.DatabaseName, dbSettings.UserName, dbSettings.UserPassword);

                Database.SetConnectionString(_connectionString);
            }
            return _connectionString;
        }
    }
}