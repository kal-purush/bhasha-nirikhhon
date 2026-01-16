namespace AireBugTrackerApi.Helpers
{
    public static class LogEvents
    {
        public const int GetItems = 2000;
        public const int GetItem = 2001;
        public const int CreateItem = 2002;
        public const int UpdateItem = 2003;
        public const int DeleteItem = 2004;

        public const int CreateItemConflict = 3002;
        public const int UpdateItemConflict = 3003;

        public const int GetItemNotFound = 4001;
        public const int UpdateItemNotFound = 4003;

        public const int GetItemsInternalError = 5000;
        public const int GetItemInternalError = 5001;
        public const int CreateItemInternalError = 5002;
        public const int UpdateItemInternalError = 5003;
        public const int DeleteItemInternalError = 5004;
    }
}