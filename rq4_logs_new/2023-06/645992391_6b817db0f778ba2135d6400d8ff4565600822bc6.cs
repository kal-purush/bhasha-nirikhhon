using System.Net;
using Company.Function;
using FirebaseAdmin.Auth;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using MySqlConnector;
using Newtonsoft.Json;

namespace Company
{
    public static class SubmitUserActivities
    {
        static SubmitUserActivities()
        {
            FirebaseInitializer.Initialize();
        }

        [Function("SubmitUserActivities")]
        public static async Task<HttpResponseData> SubmitInformationAsync([HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "submit-user-activity")] HttpRequestData req)
        {
            var responseBodyObject = new {
                reply = "InternalError"
            };
            try
            {
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                dynamic data = JsonConvert.DeserializeObject(requestBody);

                // Get "input" parameter from HTTP request as either parameter or post value
                string userKey = req.Query["userKey"];
                userKey ??= data?.userKey;

                List<dynamic> transports = data.Transports;
                List<dynamic> foods = data.Foods;
                List<dynamic> utilities = data.Utilities;

                UserRecord userRecord = await FirebaseAuth.DefaultInstance.GetUserAsync(userKey);
                // See the UserRecord reference doc for the contents of userRecord.

                try
                {
                    // find userKey and return the ID
                    int ID = FindUser(userKey);
                    if (ID == -1) {
                        responseBodyObject = new {
                            reply = "UserNotFound"
                        };
                        return await HttpResponseDataFactory.GetHttpResponseData(req, HttpStatusCode.OK, responseBodyObject);
                    }

                    // Check if the database has a user with the same userKey
                    bool isActivitiesAdded = SubmitActivities(ID, transports, foods, utilities);
                    if (isActivitiesAdded) {
                        responseBodyObject = new {
                            reply = "Success"
                        };
                        return await HttpResponseDataFactory.GetHttpResponseData(req, HttpStatusCode.OK, responseBodyObject);
                    } else {
                        return await HttpResponseDataFactory.GetHttpResponseData(req, HttpStatusCode.InternalServerError, responseBodyObject);
                    }
                }
                catch (Exception) {
                    return await HttpResponseDataFactory.GetHttpResponseData(req, HttpStatusCode.InternalServerError, responseBodyObject);
                }
            }
            catch (ArgumentException) {
                responseBodyObject = new {
                    reply = "InvalidArgument"
                };
                return await HttpResponseDataFactory.GetHttpResponseData(req, HttpStatusCode.BadRequest, responseBodyObject);
            }
            catch (FirebaseAuthException) {
                responseBodyObject = new {
                    reply = "UserKeyNotAuth"
                };
                return await HttpResponseDataFactory.GetHttpResponseData(req, HttpStatusCode.BadRequest, responseBodyObject);
            }
            throw new NotImplementedException();
        }
        private static int FindUser(string userKey)
        {
            int ID = -1;
            const string QUERY = "SELECT * From Users";

            // open connection
            MySqlConnection connection = DatabaseConnecter.MySQLDatabase();
            connection.Open();

            // set command and reader at the Users Table
            using MySqlCommand command = new(QUERY, connection);
            using MySqlDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                int currID = reader.GetInt32("ID");
                string currUserKey = reader.GetString("UserKey");

                bool found = BCrypt.Net.BCrypt.Verify(userKey, currUserKey);
                if (found) {
                    ID = currID;
                    break;
                }
            }
            command.Transaction.Commit();
            return ID;
        }
        public static bool SubmitActivities(int userID, List<dynamic> transports, List<dynamic> foods, List<dynamic> utilities) {
            MySqlConnection connection = DatabaseConnecter.MySQLDatabase();
            using(connection)
            {
                connection.Open();
                using MySqlTransaction transaction = connection.BeginTransaction();
                try
                {
                    DateTime todaysDate = DateTime.UtcNow;
                    string date = todaysDate.ToString("yyyy/MM/dd");

                    // delete previous entries
                    DeleteFromDatabase(connection, userID, date);

                    // check if the new parameters are valid numbers
                    bool validatedNumbers = ValidateNumbers(transports, foods, utilities);
                    if (!validatedNumbers) {
                        throw new Exception();
                    }

                    // input new entries using same query
                    const string QUERY = "INSERT INTO Activities (UserID, ActivityType, Method, Amount, DateTime, Emission) Values (@UserID, @ActivityType, @Method, @Amount, @DateTime, @Emission)";
                    InputTransport(userID, transports, connection, date, QUERY);
                    InputFood(userID, foods, connection, date, QUERY);
                    InputUtilities(userID, utilities, connection, date, QUERY);

                    // update Daily emissions
                    UpdateDailyEmissions(userID, connection, date);

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
        private static bool ValidateNumbers(List<dynamic> transports, List<dynamic> foods, List<dynamic> utilities)
        {
            // call each individual amount validator
            bool transportsValid = ValidateTransportAmounts(transports);
            bool foodsValid = ValidateFoodAmounts(foods);
            bool utilitiesValid = ValidateUtilityAmounts(utilities);

            // check if any are false, return false, otherwise true
            return transportsValid && foodsValid && utilitiesValid;
        }
        private static bool ValidateTransportAmounts(List<dynamic> transports)
        {
            // go through each amount in transports and check if its a number variable
            foreach (var transport in transports)
            {
                string amount = transport.Miles;
                if (!double.TryParse(amount, out _))
                {
                    return false; // Invalid non-number value found, return false
                }
            }
            return true; // All amounts are valid numbers
        }
        private static bool ValidateFoodAmounts(List<dynamic> foods)
        {
            // go through each amount in foods and check if its a number variable
            foreach (var food in foods)
            {
                string amount = food.Amount;
                if (!double.TryParse(amount, out _))
                {
                    return false; // Invalid non-number value found, return false
                }
            }
            return true; // All amounts are valid numbers
        }
        private static bool ValidateUtilityAmounts(List<dynamic> utilities)
        {
            // go through each utilities in transports and check if its a number variable
            foreach (var utility in utilities)
            {
                string amount = utility.Hours;
                if (!double.TryParse(amount, out _))
                {
                    return false; // Invalid non-number value found, return false
                }
            }
            return true; // All amounts are valid numbers
        }
        public static void DeleteFromDatabase(MySqlConnection connection, int userID, string date) {
            // new command
            string query = "DELETE FROM Activities WHERE UserID = '" + userID + "' AND DateTime = '" + date + "'";
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
        public static void InputTransport(int userID, List<dynamic> transports, MySqlConnection connection, string date, string QUERY) {
            // new command
            using MySqlCommand command = new(QUERY, connection);
            // start a transaction
            command.Transaction = connection.BeginTransaction();

            // varaibles that dont change for each unique transport
            const string ACTIVITY_TYPE = "transport";
            EmissionCalculator calculations = new();

            try
            {
            // add each entry in the table
            foreach (var transport in transports)
            {
                // transportation type
                string method = transport.TransportType;
                // amount of miles driven
                double amount = transport.Miles;

                // calculate emissions with, method (transportation type), and amount (miles driven)
                double emission = calculations.DrivingCalculation(method, amount);

                // insert info in Activities Table
                command.CommandText = QUERY;
                command.Parameters.AddWithValue("@UserID", userID);
                command.Parameters.AddWithValue("@ActivityType", ACTIVITY_TYPE);
                command.Parameters.AddWithValue("@Method", method);
                command.Parameters.AddWithValue("@Amount", amount);
                command.Parameters.AddWithValue("@DateTime", date);
                command.Parameters.AddWithValue("@Emission", emission);
                command.ExecuteNonQuery();
            }
            // commit transction
            command.Transaction.Commit();
            }
            catch (Exception) {
                command.Transaction.Rollback();
                throw new Exception(); // makes SubmitActivities rollback
            }
        }
        public  static void InputFood(int userID, List<dynamic> foods, MySqlConnection connection, string date, string QUERY) {
            // new command
            using MySqlCommand command = new(QUERY, connection);
            // start transaction
            command.Transaction = connection.BeginTransaction();
            // varaibles that dont change for each unique foods
            const string ACTIVITY_TYPE = "foods";
            EmissionCalculator calculations = new();

            try
            {
            // add each food in foods
            foreach (var food in foods)
            {
                // food type eaten
                string method = food.FoodType;
                // amount of food eaten
                double amount = food.Amount;

                // calculate emissions, method (foodType), amount (servings or grams)
                double emission = calculations.FoodCalculation(method, amount);

                // insert info in Activities Table
                command.CommandText = QUERY;
                command.Parameters.AddWithValue("@UserID", userID);
                command.Parameters.AddWithValue("@ActivityType", ACTIVITY_TYPE);
                command.Parameters.AddWithValue("@Method", method);
                command.Parameters.AddWithValue("@Amount", amount);
                command.Parameters.AddWithValue("@DateTime", date);
                command.Parameters.AddWithValue("@Emission", emission);
                command.ExecuteNonQuery();
            }
            // commit transaction
            command.Transaction.Commit();
            }
            catch (Exception) {
                command.Transaction.Rollback();
                throw new Exception(); // makes SubmitActivities rollback
            }
        }
        public static void InputUtilities(int userID, List<dynamic> utilities, MySqlConnection connection, string date, string QUERY) {
            // new command
            using MySqlCommand command = new(QUERY, connection);
            // start a transaction
            command.Transaction = connection.BeginTransaction();
            // varaibles that dont change for each unique foods
            const string ACTIVITY_TYPE = "utility";
            EmissionCalculator calculations = new();
            try
            {
                foreach (var utility in utilities)
                {
                    // get the utility type
                    string method = utility.UtilityType;
                    // get hours used
                    double amount = utility.Hours;
                    // calculate the utilities emission
                    double emission = calculations.UtilitiesCalculation(method, amount);
                    if (emission == -1)
                    {
                        break;
                    }

                    // insert info in Activities Table
                    command.CommandText = QUERY;
                    command.Parameters.AddWithValue("@UserID", userID);
                    command.Parameters.AddWithValue("@ActivityType", ACTIVITY_TYPE);
                    command.Parameters.AddWithValue("@Method", method);
                    command.Parameters.AddWithValue("@Amount", amount);
                    command.Parameters.AddWithValue("@DateTime", date);
                    command.Parameters.AddWithValue("@Emission", emission);
                    command.ExecuteNonQuery();
                }
            // commit transaction
            command.Transaction.Commit();
            }
            catch (Exception) {
                command.Transaction.Rollback();
                throw new Exception(); // makes SubmitActivities rollback
            }
        }
        private static void UpdateDailyEmissions(int userID, MySqlConnection connection, string date)
        {
            // get the total Emissions from the day
            double addedTotalEmissions = GetTotalEmissions(userID, connection, date);
            UpdateOrSetDailyEmissions(userID, connection, date, addedTotalEmissions);
        }
        private static double GetTotalEmissions(int userID, MySqlConnection connection, string date)
        {
            // new command
            string query = "SELECT Emission FROM Activities WHERE UserID = '" + userID + "' AND DateTime = '" + date + "'";
            using MySqlCommand command = new(query, connection);
            // start a transaction
            command.Transaction = connection.BeginTransaction();

            try {
                double totalAddedEmissions = 0;
                // set command and reader at the Users Table
                command.CommandText = query;
                using MySqlDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    double currEmission = reader.GetDouble("Emission");
                    totalAddedEmissions += currEmission;
                }
                command.Transaction.Commit();
                return totalAddedEmissions;
            }
            catch (Exception) {
                command.Transaction.Rollback();
                throw new Exception();
            }
        }
         private static void UpdateOrSetDailyEmissions(int userID, MySqlConnection connection, string date, double addedTotalEmissions)
        {
            // read table DailyEmissions
            string query = "SELECT * FROM DailyEmissions WHERE UserID = '" + userID + "' AND DateTime = '" + date + "' FOR UPDATE";
            using MySqlCommand command = new(query, connection);
            // start a transaciton 
            command.Transaction = connection.BeginTransaction();
            try
            {
                using MySqlDataReader reader = command.ExecuteReader();
                if (reader.Read())
                {
                    // Entry exists, update TotalAmount
                    double currentTotalAmount = reader.GetDouble("TotalAmount");
                    double newTotalAmount = currentTotalAmount + addedTotalEmissions;

                    // Update the TotalAmount for the existing entry
                    string updateQuery = "UPDATE DailyEmissions SET TotalAmount = @TotalAmount WHERE UserID = '" + userID + "' AND DateTime = '" + date + "'";

                    // command to update table DailyEmissions
                    using MySqlCommand updateCommand = new(updateQuery, connection);
                    // start a transaction
                    updateCommand.Transaction = connection.BeginTransaction();

                    try
                    {
                        updateCommand.Parameters.AddWithValue("@TotalAmount", newTotalAmount);
                        updateCommand.Parameters.AddWithValue("@Date", date);
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
                        insertCommand.Parameters.AddWithValue("@TotalAmount", addedTotalEmissions);
                        const double GOAL = 60.4;
                        insertCommand.Parameters.AddWithValue("@Goal", GOAL);
                        insertCommand.Parameters.AddWithValue("@Date", date);
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