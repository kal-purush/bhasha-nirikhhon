using ClassLibrary1.Controller;
using System.Security.Cryptography;
using System.Text;

namespace ClassLibrary1.Model
{
    /// <summary>
    /// This is the class PasswordHasher.
    /// </summary>
    public class PasswordHasher
    {

        /// <param name="hash"></param>
        /// <returns></returns>
        private static string GetStringFromHash(byte[] hash)
        {
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < hash.Length; i++)
            {
                result.Append(hash[i].ToString("X2"));
            }
            return result.ToString();
        }
        /// <summary>
        /// Hashes a given password.
        /// </summary>
        /// <param name="password"></param>
        /// <returns></returns>
        public string HashPassword(string password)
        {
            using (SHA512 sha512 = SHA512Managed.Create())
            {
                byte[] bytes = Encoding.UTF8.GetBytes(password);
                byte[] hash = sha512.ComputeHash(bytes);
                return GetStringFromHash(hash);
            }
        }

        /// <summary>
        /// Validates a given password.
        /// </summary>
        /// <param name="user"></param>
        /// <returns></returns>
        public bool ValidatePassword(CustomUser user)
        {
            CustomUserController ctrl = new CustomUserController();

            CustomUser storedUser = ctrl.FindByUsername(user.Username);

            string storedHash = storedUser.Password;

            if (user.Password == storedHash && storedUser.IsActive == true)
            {
                return true;
            }

            else
            {
                return false;
            }
        }
    }
}