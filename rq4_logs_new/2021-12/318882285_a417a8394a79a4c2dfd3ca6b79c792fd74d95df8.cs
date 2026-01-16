using Dapper;
using employers.domain.Entities.UserAuth;
using employers.domain.Interfaces.Repositories.UserAuth;
using Microsoft.Extensions.Configuration;
using System;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.Threading.Tasks;

namespace employers.infrastructure.Repositories.UserAuth
{
    public class UserRepository : IUserRepository
    {
        private readonly IConfiguration _configuration;
        private readonly StringBuilder _stringBuilder;
        public IDbConnection Connection
        {
            get
            {
                return new SqlConnection(_configuration.GetConnectionString("sqlConnect"));
            }
        }

        public UserRepository(IConfiguration configuration)
        {
            _stringBuilder = new StringBuilder();
            _configuration = configuration;
        }

        public Task<bool> DisableUser(int id)
        {
            throw new NotImplementedException();
        }

        public async Task<int> InsertUser(UserEntity userEntity)
        {
            _stringBuilder.Append($"INSERT INTO User_Auth ");
            _stringBuilder.Append($"VALUES ('{userEntity.UserName}', '{userEntity.FullName}', '{userEntity.Password}', '', '', '')");

            var result = await Connection.ExecuteAsync(_stringBuilder.ToString());

            return result;
        }
    }
}