using System.Diagnostics;
using System.Linq;
using TAB_clinic_Data.Database;

namespace TAB_clinic_Business
{
    public static class LoginService
    {
        static LoginService()
        {
            using (var context = new ClinicDBContext())
            {
                var admin = context.Users
                    .Where(u => u.Login == "admin")
                    .FirstOrDefault();

                Trace.WriteLine("Admin was found in the DB");

                if (admin is null)
                {
                    Trace.WriteLine("Admin was not found in the DB - adding now");
                    admin = new User
                    {
                        Login = "admin",
                        Password = "admin",
                        Role = null
                    };

                    context.Users.Add(admin);
                    context.SaveChanges();
                }
            }
        }

        public static bool SignIn(string login, string password)
        {
            using (var context = new ClinicDBContext())
            {
                var matchingUser = context.Users
                    .Where(u => u.Login == login && u.Password == password)
                    .FirstOrDefault();

                return (matchingUser is not null);
            }
        }

        public static string AdminsPassword()
        {
            User admin;
            using (var context = new ClinicDBContext())
            {
                admin = context.Users
                    .Where(u => u.Login == "admin")
                    .FirstOrDefault();
            }
            return admin.Password;
        }
    }
}