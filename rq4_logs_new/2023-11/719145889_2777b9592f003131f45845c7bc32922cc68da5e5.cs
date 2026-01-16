namespace legallead.jdbc
{
    internal static class LocalData
    {
        private const string PostGresCommand = "Host=localhost;Username=<username>;Password=<password>;Database=legallead_local";

        private static string[] GetPassCode()
        {
            const string saltLocal = "local.lead.pgre.passcode";
            const string conversion = "PJ+tGCWgm71p1EuzljbETc7jK+cWSiBChGnloUKeMWY=";
            const string vector = "oPo5iwHj+8W68av+FTRbjw==";
            return CryptoManager.Decrypt(conversion, saltLocal, vector).Split('|');
        }

        public static string GetPostGreString()
        {
            var secret = GetPassCode();
            var connection = PostGresCommand;
            connection = connection.Replace("<username>", secret[0]);
            connection = connection.Replace("<password>", secret[1]);
            return connection;
        }
    }
}