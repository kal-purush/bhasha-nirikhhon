using Dapper;
using legallead.jdbc.interfaces;
using System.Data;

namespace legallead.jdbc.implementations
{
    internal class DapperExecutor : IDapperCommand
    {
        public async Task ExecuteAsync(IDbConnection conn, string sql, params object[] args)
        {
            await conn.ExecuteAsync(sql, args);
        }

        public async Task<IEnumerable<T>> QueryAsync<T>(IDbConnection conn, string sql, params object[] args)
        {
            var response = await conn.QueryAsync<T>(sql, new { args });
            return response;
        }

        public async Task<T?> QuerySingleOrDefaultAsync<T>(IDbConnection conn, string sql, params object[] args)
        {
            var response = await conn.QuerySingleOrDefaultAsync<T>(sql, new { args });
            return response;
        }
    }
}