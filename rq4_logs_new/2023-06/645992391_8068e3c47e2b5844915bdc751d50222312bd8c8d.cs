using Company.Function;
using GetEmissionHistoryUtils;
using MySqlConnector;

namespace BackEndFucntions
{
    public static class UserOneYearDailyEmissions
    {
        public static async Task<List<DailyEmission>> GetEmissions(int userId)
        {
            DateTime currentDate = DateTime.UtcNow;
            DateTime oneYearAgo = currentDate.AddYears(-1);

            MySqlConnection connection = DatabaseConnecter.MySQLDatabase();

            using (connection)
            {
                connection.Open();

                const string query = "SELECT e.DateTime, e.TotalAmount, e.Goal " +
                    "FROM DailyEmission AS e " +
                    "JOIN Users AS u ON e.UserID = u.ID " +
                    "WHERE u.ID = @userId AND e.DateTime >= '@oneYearAgo' AND e.DateTime <= '@currentDate'" +
                    " ORDER BY e.DateTime ASC";

                using MySqlCommand command = connection.CreateCommand();
                command.CommandText = query;
                command.Parameters.AddWithValue("@userId", userId);
                command.Parameters.AddWithValue("@oneYearAgo", oneYearAgo.ToString("yyyy/MM/dd"));
                command.Parameters.AddWithValue("@currentDate", currentDate.ToString("yyyy/MM/dd"));

                using MySqlDataReader reader = await command.ExecuteReaderAsync();
                List<DailyEmission> emissions = new ();
                const string DATE_TIME_COL = "DateTime";
                const string TOTAL_AMOUNT_COL = "TotalAmount";
                const string GOAL_COL = "Goal";

                if (reader.HasRows)
                {
                while (reader.Read())
                {
                    // Store a list of DailyEmission
                    DateTime dateTime = reader.GetDateTime(DATE_TIME_COL).Date;
                    double total = reader.GetDouble(TOTAL_AMOUNT_COL);
                    double goal = reader.GetDouble(GOAL_COL);

                    emissions.Add(new DailyEmission { DateTime = dateTime, Total = total, Goal = goal });
                }

                // Fill in the missing days with null total and default goal
                const int ZERO_DATE_OFFSET = 0;
                List<DateTime> allDates = Enumerable.Range(ZERO_DATE_OFFSET, (currentDate - oneYearAgo).Days)
                    .Select(offset => oneYearAgo.AddDays(offset).Date)
                    .ToList();

                List<DateTime> existingDates = emissions.ConvertAll(new Converter<DailyEmission, DateTime>(ConvertToDateTime.Convert));

                List<DateTime> missingDates = allDates.Except(existingDates).ToList();

                emissions.AddRange(missingDates.ConvertAll(new Converter<DateTime, DailyEmission>(ConvertToEmptyEmission.Convert)));
                connection.Close();
                return emissions.OrderBy(e => e.DateTime).ToList();
                }
                // Return empty history if there are none within an year
                connection.Close();
                return emissions;
            }
        }
    }
}