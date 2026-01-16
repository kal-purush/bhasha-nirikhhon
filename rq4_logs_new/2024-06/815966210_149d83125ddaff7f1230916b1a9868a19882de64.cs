using System.Security.Cryptography;


namespace APISendIt.helper
{
    public static class PasswordHelperAPI
    {
        public static string HashPassword(string password, out byte[] salt)
        {
            salt = new byte[16];
            using (var rng = new RNGCryptoServiceProvider())
            {
                rng.GetBytes(salt);
            }

            var pbkdf2 = new Rfc2898DeriveBytes(password, salt, 10000);
            byte[] hash = pbkdf2.GetBytes(20);

            byte[] hashBytes = new byte[36];
            Array.Copy(salt, 0, hashBytes, 0, 16);
            Array.Copy(hash, 0, hashBytes, 16, 20);
            Console.WriteLine("salt Hash Password: " + salt);
            Console.WriteLine("pbkdf2 Hash Password: " + pbkdf2);
            Console.WriteLine("hashBytes Hash Password: " + hashBytes);

            return Convert.ToBase64String(hashBytes);


        }

        public static bool VerifyPassword(string password, string storedHash)
        {
            byte[] hashBytes = Convert.FromBase64String(storedHash);

            byte[] salt = new byte[16];
            Array.Copy(hashBytes, 0, salt, 0, 16);

            var pbkdf2 = new Rfc2898DeriveBytes(password, salt, 10000);
            byte[] hash = pbkdf2.GetBytes(20);

            for (int i = 0; i < 20; i++)
            {
                if (hashBytes[i + 16] != hash[i])
                    return false;
            }
            Console.WriteLine("salt Verify Password: " + salt);
            Console.WriteLine("pbkdf2 Verify Password: " + pbkdf2);
            Console.WriteLine("hashBytes Verify Password: " + hashBytes);

            return true;


        }
    }

}