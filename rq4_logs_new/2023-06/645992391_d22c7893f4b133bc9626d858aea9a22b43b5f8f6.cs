using MySqlConnector;

namespace UserKeyUtils
{
    static class InsertUserKey
    {
        public static bool Insert(MySqlConnection connection, MySqlTransaction transaction, string hashedUserKey)
        {
            using MySqlCommand command = connection.CreateCommand();
            const string userKey = "@userKey";
            const string QUERY = "INSERT INTO Users (UserKey) Values (" + userKey + ")";
            command.CommandText = QUERY;
            command.Transaction = transaction;
            command.Parameters.AddWithValue(userKey, hashedUserKey);
            command.ExecuteNonQuery();

            // Commit the transaction
            transaction.Commit();

            // Transaction completed successfully
            connection.Close();
            return true;
        }
    }
}