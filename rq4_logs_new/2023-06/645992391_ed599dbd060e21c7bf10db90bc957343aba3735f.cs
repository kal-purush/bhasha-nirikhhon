using System.Net;
using FirebaseAdmin.Auth;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using MySqlConnector;
using Newtonsoft.Json;

namespace Company.Function
{
    public class SubmitUserActivity
    {

        public SubmitUserActivity()
        {
            FirebaseInitializer.Initialize();
        }

        [Function("SubmitUserActivity")]
        public static async Task<IActionResult> SubmitInformationAsync([HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "submit-user-activity")] HttpRequestData req)
        {
           try
           {
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                dynamic data = JsonConvert.DeserializeObject(requestBody);

                // Get "input" parameter from HTTP request as either parameter or post value
                string userKey = req.Query["UserKey"];
                userKey ??= data?.UserKey;

                List<dynamic> transports = data.Transports;
                List<dynamic> foods = data.Foods;
                List<dynamic> utilities = data.Utilities;

                UserRecord userRecord = await FirebaseAuth.DefaultInstance.GetUserAsync(userKey);
                // See the UserRecord reference doc for the contents of userRecord.

                try
                {
                    // Hash the userKey
                    string hashedUserKey = BCrypt.Net.BCrypt.HashPassword(userKey);

                    // Check if the database has a user with the same hashedUserKey
                    bool isUserAdded = SubmitActivities(userKey, transports, foods, utilities);
                    if (isUserAdded) {
                        return new OkObjectResult("Success");
                    }

                    // For some reason, the userkey is not added to the database
                    return new BadRequestObjectResult("InternalError");
                }
                catch (Exception) {
                    return new BadRequestObjectResult("InternalError");
                }
           }
           catch (ArgumentException) {
                return new BadRequestObjectResult("InvalidArgument");
            }
            catch (FirebaseAuthException) {
                return new BadRequestObjectResult("UserKeyNotAuth");
            }
            throw new NotImplementedException();
        }

        public static bool SubmitActivities(string hashedUserKey, List<dynamic> transports, List<dynamic> foods, List<dynamic> utilities ) {
            MySqlConnection connection = DatabaseConnecter.ConnectToMySql();
            using(connection)
            {
                connection.Open();
                MySqlTransaction transaction = connection.BeginTransaction();
                try
                {
                    MySqlCommand command = connection.CreateCommand();
                    command.Transaction = transaction;
                    DateTime todaysDate = DateTime.UtcNow;
                    string date = todaysDate.ToString("yyyy/MM/dd");

                    // delete previous entries
                    DeleteFromDatabase(command, hashedUserKey, date);

                    //input new entries
                    InputTransport(transports, command, hashedUserKey, date);
                    InputFood(foods, command, hashedUserKey, date);
                    InputUtilities(utilities, command, hashedUserKey, date);

                    //update Daily emissions

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
        public static void DeleteFromDatabase(MySqlCommand command, string hashedUserKey, string date) {
            string query = "DELETE FROM Activities WHERE UserID = '" + hashedUserKey + "' AND DateTime = '" + date + "';";
            command.CommandText = query;
            command.ExecuteNonQuery();
        }

        public static void InputTransport(List<dynamic> transports, MySqlCommand command, string hashedUserKey, string date) {
            foreach (var transport in transports)
            {
                string transportType = transport.TransportType;
                string miles = transport.Miles;

                string userID = "";
                const string activityType = "transport";
                int method = 0;
                double amount = 0.0;
                double emission = 0.0;

                const string query = "INSERT INTO Activities (ID, UserID, ActivityType, Method, Amount, DateTime, Emission) Values (@ID, @UserID, @ActivityType, @Method, @Amount, @DateTime, @Emission)";
                command.CommandText = query;
                command.Parameters.AddWithValue("@ID", hashedUserKey);
                command.Parameters.AddWithValue("@UserID", userID);
                command.Parameters.AddWithValue("@ActivityType", activityType);
                command.Parameters.AddWithValue("@Method", method);
                command.Parameters.AddWithValue("@Amount", amount);
                command.Parameters.AddWithValue("@DateTime", date);
                command.Parameters.AddWithValue("@Emission", emission);
                command.ExecuteNonQuery();
            }
        }
        public  static void InputFood(List<dynamic> foods, MySqlCommand command, string hashedUserKey, string date) {
            foreach (var food in foods)
            {
                string foodType = food.FoodType;
                string amount = food.Amount;

                string userID = "";
                string activityType = "";
                int method = 0;
                double emission = 0.0;

                string query2 = "INSERT INTO Activities (ID, UserID, ActivityType, Method, Amount, DateTime, Emission) Values (@ID, @UserID, @ActivityType, @Method, @Amount, @DateTime, @Emission)";
                command.Parameters.AddWithValue("@ID", hashedUserKey);
                command.Parameters.AddWithValue("@UserID", userID);
                command.Parameters.AddWithValue("@ActivityType", activityType);
                command.Parameters.AddWithValue("@Method", method);
                command.Parameters.AddWithValue("@Amount", amount);
                command.Parameters.AddWithValue("@DateTime", date);
                command.Parameters.AddWithValue("@Emission", emission);
                command.ExecuteNonQuery();
            }
        }
        public static void InputUtilities(List<dynamic> utilities, MySqlCommand command, string hashedUserKey, string date) {
            foreach (var utility in utilities)
            {
                string transportType = utility.UtilityType;
                string miles = utility.Hours;

                string userID = "";
                string activityType = "";
                int method = 0;
                double amount = 0.0;
                double emission = 0.0;

                string query2 = "INSERT INTO Activities (ID, UserID, ActivityType, Method, Amount, DateTime, Emission) Values (@ID, @UserID, @ActivityType, @Method, @Amount, @DateTime, @Emission)";
                command.Parameters.AddWithValue("@ID", hashedUserKey);
                command.Parameters.AddWithValue("@UserID", userID);
                command.Parameters.AddWithValue("@ActivityType", activityType);
                command.Parameters.AddWithValue("@Method", method);
                command.Parameters.AddWithValue("@Amount", amount);
                command.Parameters.AddWithValue("@DateTime", date);
                command.Parameters.AddWithValue("@Emission", emission);
                command.ExecuteNonQuery();
            }
        }
    }
}