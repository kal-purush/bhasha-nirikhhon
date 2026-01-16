using System.Net;
using BackEndFunctions;
using FirebaseAdmin.Auth;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using MySqlConnector;
using NoCO2.Function;
using NoCO2.Util;

namespace Company.Function
{
    public class GetUserActivitie
    {
        class ReturnedInfo
        {
            public List<dynamic> Transports { get; }
            public List<dynamic> Foods { get; }
            public List<dynamic> Utilities { get; }
            public ReturnedInfo(List<dynamic> transports, List<dynamic> foods, List<dynamic> utilities)
            {
                Transports = transports;
                Foods = foods;
                Utilities = utilities;
            }
        }
        public GetUserActivitie()
        {
            FirebaseInitializer.Initialize();
        }

        [Function("GetUserActivitie")]
        public async Task<HttpResponseData> GetActivities([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = = "get-user-activities")] HttpRequestData req)
        {
            var responseBodyObject = new {
                reply = "InternalError"
            };
            try
            {
                req.Body.TryParseJson<GeneralUserKeyBody>(out var requestBody);

                // Get "input" parameter from HTTP request as either parameter or post value
                string userKey = requestBody?.UserKey;

                UserRecord userRecord = await FirebaseAuth.DefaultInstance.GetUserAsync(userKey);
                // See the UserRecord reference doc for the contents of userRecord.

                try
                {
                    // find userKey and return the ID
                    int matchedUserID = FindUser.UserFinder(userKey);
                    if (matchedUserID == -1) {
                        responseBodyObject = new {
                            reply = "UserNotFound"
                        };
                        return await HttpResponseDataFactory.GetHttpResponseData(req, HttpStatusCode.BadRequest, responseBodyObject);
                    }
                    ReturnedInfo information = RetrieveAllUserActivities(matchedUserID);
                    var successresponseBodyObject = new {
                        reply = "Success",
                        Transports = information.Transports,
                        Foods = information.Foods,
                        Utilities = information.Utilities
                    };
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
            catch (Exception) {
                return await HttpResponseDataFactory.GetHttpResponseData(req, HttpStatusCode.InternalServerError, responseBodyObject);
            }
            throw new NotImplementedException();
        }

        private static ReturnedInfo RetrieveAllUserActivities(int userID)
        {
            DateTime today = DateTime.UtcNow;
            const string TRANSPORT = "transport";
            const string FOOD = "food";
            const string UTILITY = "utility";
            List<dynamic> Transports = RetrieveCertainType(userID, today, TRANSPORT);
            List<dynamic> Foods = RetrieveCertainType(userID, today, FOOD);
            List<dynamic> Utilities = RetrieveCertainType(userID, today, UTILITY);
            return new ReturnedInfo(Transports, Foods, Utilities);
        }

        public static List<dynamic> RetrieveCertainType(int userID, DateTime today, string ACTIVITY_TYPE)
        {
            List<dynamic> Transports = null;
            string QUERY = "SELECT FROM Activities WHERE UserID = '" + userID + "' AND DateTime = '" + today.ToString("yyyy/MM/dd") + "' AND ActivityType = '" + ACTIVITY_TYPE + "'";

            using MySqlConnection connetion = DatabaseConnecter.MySQLDatabase();
            connetion.Open();

            using MySqlCommand command = new(QUERY, connetion);

            try {
                command.CommandText = QUERY;
                using MySqlDataReader reader = command.ExecuteReader();
                while (reader.Read()) {
                    string type = reader.GetString("Method");
                    double amount = reader.GetDouble("Amount");
                    Transports.Add(new {Type = type, Amount = amount});
                }
                return Transports;
            }
            catch (Exception) {
                throw new Exception();
            }
        }
    }
}