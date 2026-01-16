using System.Collections.Generic;
using System.Threading.Tasks;
using CarsInfo.DAL.Contracts;
using CarsInfo.DAL.Entities;

namespace CarsInfo.DAL.Repositories
{
    public class UsersRepository : GenericRepository<User>, IUsersRepository
    {
        public UsersRepository(IDbContext context) 
            : base(context)
        { }

        public async Task<IEnumerable<User>> GetAllWithRolesAsync()
        {
            var sql = @$"SELECT * FROM {TableName} u
                         INNER JOIN UserRole ur 
                         ON u.UserId = ur.UserId
                         INNER JOIN Role r
                         ON ur.RoleId = r.Id";

            return await Context.QueryAsync<User, Role>(sql,
                (user, role) =>
                {
                    user.Roles.Add(role);
                    return user;
                });
        }

        public async Task<User> GetWithRolesAsync(string email)
        {
            var sql = @$"SELECT TOP 1 * FROM {TableName} u
                         INNER JOIN UserRole ur 
                         ON u.UserId = ur.UserId
                         INNER JOIN Role r
                         ON ur.RoleId = r.Id
                         WHERE u.Email = {email}";

            return await Context.QueryFirstOrDefaultAsync<User, Role>(sql,
                (user, role) =>
                {
                    user.Roles.Add(role);
                    return user;
                });
        }
    }
}