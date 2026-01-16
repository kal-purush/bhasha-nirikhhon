namespace GetUserActivitiesUtil
{
    public static class AllActivityRetrieval
    {
        public static ReturnedInfo Retrieve(int userID)
        {
            DateTime today = DateTime.UtcNow;
            const string TRANSPORT = "transport";
            const string FOOD = "food";
            const string UTILITY = "utility";

            // use data retrieval class to retrieve specfic type data of each activity
            DataRetrieval retrieveData = new();
            List<dynamic> Transports = retrieveData.RetrieveCertainType(userID, today, TRANSPORT);
            List<dynamic> Foods = retrieveData.RetrieveCertainType(userID, today, FOOD);
            List<dynamic> Utilities = retrieveData.RetrieveCertainType(userID, today, UTILITY);
            return new ReturnedInfo(Transports, Foods, Utilities);
        }
    }
}