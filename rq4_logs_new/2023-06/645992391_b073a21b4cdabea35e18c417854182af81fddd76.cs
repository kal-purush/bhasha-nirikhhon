using MySqlConnector;

namespace SubmitUserActivitiesUtil
{
    public static class DailyEmissionsUpdate
    {
        public static void DailyEmissionsHelper(int userID, MySqlConnection connection, DateTime todaysDate)
        {
            // get the total Emissions from the day
            double totalEmissions = GetTotalEmissions(userID, connection, todaysDate);
            UpdateOrSetDailyEmissions(userID, connection, todaysDate, totalEmissions);
        }
        private static double GetTotalEmissions(int userID, MySqlConnection connection, DateTime todaysDate)
        {
            // new command
            string query = "SELECT Emission FROM Activities WHERE UserID = '" + userID + "' AND DateTime = '" + todaysDate + "'";
            using MySqlCommand command = new(query, connection);
            // start a transaction
            command.Transaction = connection.BeginTransaction();

            try {
                double totalEmissions = 0;
                // set command and reader at the Users Table
                command.CommandText = query;
                using MySqlDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    double currEmission = reader.GetDouble("Emission");
                    totalEmissions += currEmission;
                }
                command.Transaction.Commit();
                return totalEmissions;
            }
            catch (Exception) {
                command.Transaction.Rollback();
                throw new Exception();
            }
        }
        private static void UpdateOrSetDailyEmissions(int userID, MySqlConnection connection, DateTime todaysDate, double totalEmissions)
        {
            // read table DailyEmissions
            string query = "SELECT * FROM DailyEmissions WHERE UserID = '" + userID + "' AND DateTime = '" + todaysDate + "' FOR UPDATE";
            using MySqlCommand command = new(query, connection);
            // start a transaciton
            command.Transaction = connection.BeginTransaction();
            try
            {
                using MySqlDataReader reader = command.ExecuteReader();
                if (reader.Read())
                {
                    // Update the TotalAmount for the existing entry
                    string updateQuery = "UPDATE DailyEmissions SET TotalAmount = @TotalAmount WHERE UserID = '" + userID + "' AND DateTime = '" + todaysDate + "'";

                    // command to update table DailyEmissions
                    using MySqlCommand updateCommand = new(updateQuery, connection);
                    // start a transaction
                    updateCommand.Transaction = connection.BeginTransaction();

                    try
                    {
                        updateCommand.Parameters.AddWithValue("@TotalAmount", totalEmissions);
                        updateCommand.Parameters.AddWithValue("@Date", todaysDate);
                        updateCommand.Parameters.AddWithValue("@UserID", userID);
                        updateCommand.ExecuteNonQuery();
                        updateCommand.Transaction.Commit();
                    }
                    catch (Exception) {
                        updateCommand.Transaction.Rollback();
                        throw new Exception(); // new exception for main command to rollback
                    }
                }
                else
                {
                    // Entry does not exist, insert a new row
                    const string INSERT_QUERY = "INSERT INTO DailyEmissions (UserID, TotalAmount, Goal, DateTime) VALUES (@UserID, @TotalAmount, @Goal, @DateTime)";

                    // command to insert new row to DailyEmissions
                    using MySqlCommand insertCommand = new(INSERT_QUERY, connection);
                    // start a transaction
                    insertCommand.Transaction = connection.BeginTransaction();

                    try
                    {
                        insertCommand.Parameters.AddWithValue("@UserID", userID);
                        insertCommand.Parameters.AddWithValue("@TotalAmount", totalEmissions);
                        const double GOAL = 60.4;
                        insertCommand.Parameters.AddWithValue("@Goal", GOAL);
                        insertCommand.Parameters.AddWithValue("@Date", todaysDate);
                        insertCommand.ExecuteNonQuery();
                        insertCommand.Transaction.Commit();
                    }
                    catch (Exception) {
                        insertCommand.Transaction.Rollback();
                        throw new Exception(); // new exception for main command to rollback
                    }
                }
            command.Transaction.Commit();
            }
            catch (Exception) {
                command.Transaction.Rollback();
                throw new Exception(); // makes SubmitActivities Rollback
            }
        }
    }
}