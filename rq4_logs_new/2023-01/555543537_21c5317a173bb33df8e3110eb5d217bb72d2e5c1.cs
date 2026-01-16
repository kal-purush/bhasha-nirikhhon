using System;
using System.Collections.Generic;
using System.Data.Entity.Core.EntityClient;
using System.Data.SqlClient;
using System.Linq;

namespace Homework18.EntityFrame
{
    public class EntityFrameworkHelpers
    {
        public static void AddUserToUsersTable(User user)
        {
            using (var context = new EntityFrameworkDbEntities(GetSqlConnectionString()))
            {
                context.Users.Add(user);
                context.SaveChanges();
            }
        }

        public static void DeleteUserFromUsersTable(User user)
        {
            using (var context = new EntityFrameworkDbEntities(GetSqlConnectionString()))
            {
                context.Users.Attach(user);
                context.Users.Remove(user);
                context.Entry(user).State = System.Data.Entity.EntityState.Deleted;
                context.SaveChanges();
            }
        }

        public static void EditUserAgeInUsersTableByUserId(int userId, int newAgeValue)
        {
            var userForUpdate = GetEntityFrameworkDbInstance(context => context.Users.Where(x => x.Id == userId))[0];
            using (var context = new EntityFrameworkDbEntities(GetSqlConnectionString()))
            {
                context.Users.Attach(userForUpdate);
                userForUpdate.Age = newAgeValue;
                context.Entry(userForUpdate).State = System.Data.Entity.EntityState.Modified;
                context.SaveChanges();
            }
        }

        public static List<User> GetEntityFrameworkDbInstance(Func<EntityFrameworkDbEntities, IQueryable<User>> func)
        {
            //GetClientsExtenedConnectionString(ConfigurationHelper.LocalDbConnectionString))
            using (var context = new EntityFrameworkDbEntities(GetSqlConnectionString()))
            {
                var results = func(context);

                return results.ToList();
            }
        }

        public static string GetSqlConnectionString()
        {
            SqlConnectionStringBuilder connectionString = new SqlConnectionStringBuilder();

            connectionString.DataSource = "NBA-112-E1-PL";
            connectionString.InitialCatalog = "entityFrameworkDb";
            connectionString.IntegratedSecurity = true;

            var csBuilder = new EntityConnectionStringBuilder();

            csBuilder.Provider = "System.Data.SqlClient";
            csBuilder.ProviderConnectionString = connectionString.ToString();

            csBuilder.Metadata = "res://*/EntityFrameworkModel.csdl|res://*/EntityFrameworkModel.ssdl|res://*/EntityFrameworkModel.msl";

            return csBuilder.ToString();
        }
    }
}