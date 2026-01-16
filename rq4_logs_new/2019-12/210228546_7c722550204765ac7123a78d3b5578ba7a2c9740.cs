namespace Carter.App.Lib.Timer
{
    using System;
    using MongoDB.Bson;

    public static class CheckExpire
    {
        public static bool IsAfter(this BsonValue start, BsonValue expirationTime)
        {
            int seconds = expirationTime.AsInt32;
            BsonDateTime endTime = new BsonDateTime(DateTime.Now.AddSeconds(-seconds));
            Console.WriteLine(start.AsBsonDateTime.CompareTo(endTime));
            return start.AsBsonDateTime.CompareTo(endTime) < 0;
        }
    }
}