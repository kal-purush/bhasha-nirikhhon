using DatabaseConnector;
using MySqlConnector;

namespace SubmitUserActivitiesUtil
{
    public static class ActivitySubmit
    {
        public static bool Submit(int userID, List<dynamic> transports, List<dynamic> foods, List<dynamic> utilities) {
            // check if the new parameters are valid numbers
            bool validatedNumbers = ValidateActivitiesInput.ValidateNumbers(transports, foods, utilities);
            if (!validatedNumbers) {
                throw new Exception();
            }

            // create connection
            MySqlConnection connection = DatabaseConnecter.MySQLDatabase();
            using(connection)
            {
                connection.Open();
                using MySqlTransaction transaction = connection.BeginTransaction();
                try
                {
                    DateTime date = DateTime.UtcNow.Date;

                    // delete previous entries
                    DeleteUserActivities.Delete(connection, userID, date);

                    // all are null no need to update database
                    if (transports.Count == 0 && foods.Count == 0 && utilities.Count == 0) {
                        return true;
                    }

                    // input new entries using same query
                    const string QUERY = "INSERT INTO Activities (UserID, ActivityType, Method, Amount, DateTime, Emission) Values (@UserID, @ActivityType, @Method, @Amount, @DateTime, @Emission)";
                    const string transport = "transport";
                    const string food = "food";
                    const string utility = "utility";

                    // input transport
                    InputActivity.Input(userID, transports, connection, date, QUERY, transport);
                    // input food
                    InputActivity.Input(userID, transports, connection, date, QUERY, food);
                    // input utility
                    InputActivity.Input(userID, transports, connection, date, QUERY, utility);

                    // update Daily emissions
                    DailyEmissionsUpdate.Update(userID, connection, date);

                    // commit and close
                    transaction.Commit();
                    connection.Close();
                }
                catch (Exception)
                {
                    transaction.Rollback();
                    connection.Close();
                    return false;
                }
            }
            return true;
        }
    }
}