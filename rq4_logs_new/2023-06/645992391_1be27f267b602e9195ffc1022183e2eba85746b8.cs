using MySqlConnector;

namespace SubmitUserActivitiesUtil
{
    public static class DeleteUserActivities
    {
        public static void Delete(MySqlConnection connection, int userID, DateTime todaysDate) {
            // new command
            string query = "DELETE FROM Activities WHERE UserID = '" + userID + "' AND DateTime = '" + todaysDate + "'";
            using MySqlCommand command = new(query, connection);
            // start transaction
            command.Transaction = connection.BeginTransaction();
            try
            {
                command.CommandText = query;
                command.ExecuteNonQuery();
                command.Transaction.Commit();
            }
            catch (Exception) {
                command.Transaction.Rollback();
                throw new Exception();
            }
        }
    }
}