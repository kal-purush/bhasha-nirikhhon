using System.Diagnostics;
using TAB_clinic_Model;

namespace TAB_clinic_Services
{
    public static class LoginService
    {
        static LoginService()
        {
            // Create admin user if one does not exist
           UserContext.CreateUser("admin", "admin", ClinicRole.Admin);
        }

        public static UserModel? SignIn(string login, string password)
        {
            var user = UserContext.FindUser(login);
            if (user is null || !user.ValidatePassword(password))
            {
                Trace.WriteLine("Signing in failed");
                return null;
            }

            return user;
        }
    }
}