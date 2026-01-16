using System.Data.Common;
using System.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Doppler.Billing.Job.Database
{
    public class DbConnectionFactory: IDbConnectionFactory
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger<DbConnectionFactory> _logger;

        public DbConnectionFactory(ILogger<DbConnectionFactory> logger, IConfiguration configuration)
        {
            _configuration = configuration;
            _logger = logger;
        }

        public DbConnection GetConnection()
        {
            _logger.LogInformation("GetConnection()");

            var connectionString = _configuration.GetConnectionString("DopplerDatabase");
            var connection = new SqlConnection(connectionString);

            _logger.LogInformation(
                "Connection DataSource: {DataSource}, Database: {Database}, ConnectionTimeout: {ConnectionTimeout}",
                connection.DataSource,
                connection.Database,
                connection.ConnectionTimeout);

            return connection;
        }
    }
}