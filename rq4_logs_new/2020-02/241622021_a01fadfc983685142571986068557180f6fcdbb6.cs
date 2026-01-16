using System;
using System.Collections.Generic;
using Dapper;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Identity;
using System.Data.Common;
using System.Data.SqlClient;

namespace IdentitySkillUp
{
    public class PluralsightUserStore : IUserStore<PluralsightUser>, IUserPasswordStore<PluralsightUser>
    {
        public static DbConnection GetOpenConnection()
        {
            var connection = new SqlConnection("Data Source=(localdb)\\MSSQLLocalDB;Initial Catalog=PluralsightDemo;Integrated Security=True;Connect Timeout=30;Encrypt=False;TrustServerCertificate=False;ApplicationIntent=ReadWrite;MultiSubnetFailover=False");
            connection.Open();
            return connection;
        }

        public async Task<IdentityResult> CreateAsync(PluralsightUser user, CancellationToken cancellationToken)
        {
            using (var connection = GetOpenConnection())
            {
                await connection.ExecuteAsync(
                    "insert into PluralsightUsers([Id], [UserName], [NormalizedUserName], [PasswordHash])"
                    + "values (@id, @userName, @mormalizedUserName, @passwordHash)",
                    new
                    {
                        id = user.Id,
                        userName = user.UserName,
                        mormalizedUserName = user.NormalizedUserName,
                        passwordHash = user.PasswordHash
                    });
            }
            return IdentityResult.Success;
        }

        public Task<IdentityResult> DeleteAsync(PluralsightUser user, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {            
        }

        public async Task<PluralsightUser> FindByIdAsync(string userId, CancellationToken cancellationToken)
        {
            using (var connection = GetOpenConnection())
            {
                return await connection.QueryFirstOrDefaultAsync<PluralsightUser>(
                    "select * From PluralsightUsers where Id=@id",
                    new { id = userId });
            }
        }

        public async Task<PluralsightUser> FindByNameAsync(string normalizedUserName, CancellationToken cancellationToken)
        {
            using (var connection = GetOpenConnection())
            {
                return await connection.QueryFirstOrDefaultAsync<PluralsightUser>(
                    "select * From PluralsightUsers where NormalizedUserName=@normalizedUserName",
                    new { normalizedUserName });
            }
        }

        public Task<string> GetNormalizedUserNameAsync(PluralsightUser user, CancellationToken cancellationToken)
        {
            return Task.FromResult(user.NormalizedUserName);
        }

        public Task<string> GetPasswordHashAsync(PluralsightUser user, CancellationToken cancellationToken)
        {
            return Task.FromResult(user.PasswordHash);
        }

        public Task<string> GetUserIdAsync(PluralsightUser user, CancellationToken cancellationToken)
        {
            return Task.FromResult(user.Id);
        }

        public Task<string> GetUserNameAsync(PluralsightUser user, CancellationToken cancellationToken)
        {
            return Task.FromResult(user.UserName);
        }

        public Task<bool> HasPasswordAsync(PluralsightUser user, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public Task SetNormalizedUserNameAsync(PluralsightUser user, string normalizedName, CancellationToken cancellationToken)
        {
            user.NormalizedUserName = normalizedName;
            return Task.CompletedTask;
        }

        public Task SetPasswordHashAsync(PluralsightUser user, string passwordHash, CancellationToken cancellationToken)
        {
            user.PasswordHash = passwordHash;
            return Task.CompletedTask;
        }

        public Task SetUserNameAsync(PluralsightUser user, string userName, CancellationToken cancellationToken)
        {
            user.UserName = userName;
            return Task.CompletedTask;
        }

        public async Task<IdentityResult> UpdateAsync(PluralsightUser user, CancellationToken cancellationToken)
        {
            using (var connection = GetOpenConnection())
            {
                await connection.ExecuteAsync(
                    "update PluralsightUsers" +
                    "set [Id] = @id," +
                    "[UserName] = @userName" +
                    "[NormalizedUserName] = @normalizedUserName" +
                    "[PasswordHash] = @passwordHash" +
                    "where [Id] = @id",
                    new
                    {
                        id = user.Id,
                        userName = user.UserName,
                        mormalizedUserName = user.NormalizedUserName,
                        passwordHash = user.PasswordHash
                    });
            }
            return IdentityResult.Success;

        }
    }
}