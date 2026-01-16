using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using Dapper;
using Microsoft.Maui.Controls;
using Microsoft.Extensions.Configuration;

namespace TechTitans.Repositories
{
    public class DatabaseOperations : IDatabaseOperations
    {
        private static readonly IConfiguration Configuration = MauiProgram.Configuration;
        public int Execute(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            using (var connection = new Microsoft.Data.SqlClient.SqlConnection(Configuration.GetConnectionString("TechTitansDev")))
            {
                connection.Open();
                return connection.Execute(sql, param, transaction, commandTimeout, commandType);
            }
        }

        public IEnumerable<T> Query<T>(string sql, object param = null, IDbTransaction transaction = null, bool buffered = true, int? commandTimeout = null, CommandType? commandType = null)
        {
            using (var connection = new Microsoft.Data.SqlClient.SqlConnection(Configuration.GetConnectionString("TechTitansDev")))
            {
                connection.Open();
                return connection.Query<T>(sql, param, transaction, buffered, commandTimeout, commandType);
            }
        }
    }
}