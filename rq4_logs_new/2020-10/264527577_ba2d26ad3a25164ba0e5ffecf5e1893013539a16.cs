namespace FS.Utils.Helpers
{
    public static class PasswordHelper
    {
        public static string Encrypt(string password)
        {
            byte[] data = System.Text.Encoding.ASCII.GetBytes(password);
            data = new System.Security.Cryptography.SHA256Managed().ComputeHash(data);
            var hash = System.Text.Encoding.ASCII.GetString(data);

            return hash;
        }

        public static bool Compare(string fromClientSidePassword, string fromDatabasePassword)
        {
            return fromDatabasePassword == fromClientSidePassword;
        }
    }
}