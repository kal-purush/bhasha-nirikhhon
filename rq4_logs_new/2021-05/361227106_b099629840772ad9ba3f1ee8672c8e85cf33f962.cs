using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using TAB_clinic_Data.Database;

namespace TAB_clinic_Model
{
    public static class UserContext
    {
        public static UserModel? FindUser(string login)
        {
            using (var context = new ClinicDBContext())
            {
                var user = context.Users
                    .Where(u => u.Login == login)
                    .FirstOrDefault();

                if (user is null)
                    return null;

                return new UserModel(user);
            }
        }

        public static void CreateUser(string login, string password, ClinicRole role)
        {
            if (FindUser(login) != null)
            {
                Trace.WriteLine($"User with login '{login}' already exists");
                throw new UserAlreadyExistsException(login);
            }

            using (var context = new ClinicDBContext())
            {
                var newUser = new User
                {
                    Login = login,
                    Password = password,
                    Role = role.RoleToString()
                };

                context.Users.Add(newUser);
                context.SaveChanges();
            }
        }

        public static List<UserModel> GetUsers()
        {
            using (var context = new ClinicDBContext())
            {
                var users = context.Users.ToList();
                var userList = new List<UserModel>();
                foreach (var u in users)
                {
                    userList.Add(new UserModel(u));
                }
                return userList;
            }

        }
    }
}