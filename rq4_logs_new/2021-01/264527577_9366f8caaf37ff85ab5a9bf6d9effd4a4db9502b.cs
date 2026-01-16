// unset

namespace FS.Data.Repositories
{
    using Dapper;
    using Domain.Core.Interfaces;
    using Microsoft.Extensions.Configuration;
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Text;
    using System.Threading.Tasks;

    public class InstallmentMovimentRepository : IInstallmentMovimentRepository
    {
        private readonly IConfiguration _configuration;

        public InstallmentMovimentRepository(IConfiguration configuration)
        {
            _configuration = configuration;
        }
        
        private const string table = "dbo.InstallmentMoviments";
        public async Task Insert(Domain.Model.InstallmentMoviment entity)
        {
            var sql = new StringBuilder();
            sql.Append($"INSERT INTO {table}");
            sql.Append(" (Id, Value, Description, Category, [Type], [Month], StartMonth, EndMonth, InstallmentsValue," +
                       " AccountId, UserId, CreatedOn)");
            sql.Append(" VALUES(@id, @value, @description, @category, @type, @month, @startMonth, @endMonth, @installmentsValue," +
                       " @accountId, @userId, @createdOn)");

            await using var connection = new SqlConnection(_configuration.GetConnectionString("DatabaseConnection"));
            connection.Open();

            var dictionary = new Dictionary<string, object>
            {
                {"@id", entity.Id}, 
                {"@value", entity.Value}, 
                {"@description", entity.Description},
                {"@category", entity.Category},
                {"@type", entity.Type},
                {"@month", entity.Months},
                {"@startMonth", entity.StartMonth},
                {"@endMonth", entity.EndMonth},
                {"@installmentsValue", entity.InstallmentsValue},
                {"@accountId", entity.AccountId},
                {"@userId", entity.UserId},
                {"@createdOn", entity.CreatedOn},
            };

            var parameters = new DynamicParameters(dictionary);
            
            await connection.ExecuteScalarAsync(sql.ToString(), parameters);

            await Task.CompletedTask;
        }
    }
}