using Company.Function;
using MySqlConnector;

namespace CreateUserUtils
{
    public static class AddUser
    {
        public static bool Add(string originalUserKey, string hashedUserKey) {
            MySqlConnection connection = DatabaseConnecter.MySQLDatabase();
            using(connection)
            connection.Open();

            // Start the transaction
            using MySqlTransaction transaction = connection.BeginTransaction();
            try
            {
                // Selet UserKey from Users where UserKey is equal to hashedUserKey
                bool NotInsert = SelectUserKey.Select(connection, transaction, originalUserKey);
                if(NotInsert)
                {
                return true;
                }

                // Insert a user to Users table with the hashedUserKey
                return InsertUserKey.Insert(connection, transaction, hashedUserKey);
            }
            catch (Exception)
            {
                // An error occurred, rollback the transaction
                transaction.Rollback();

                // Handle the exception
                connection.Close();
                return false;
            }
        }
    }
}