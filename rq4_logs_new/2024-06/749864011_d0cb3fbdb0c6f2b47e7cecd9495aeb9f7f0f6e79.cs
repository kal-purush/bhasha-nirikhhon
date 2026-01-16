using Newtonsoft.Json.Serialization;

namespace seaway.API.Models
{
    public static class LogMessages
    {
        public const string GetRoomDataError = "An exception occurred while get all data from Room --> ";
        public const string FindRoomByIdError = "An exception occurred while get room data with Id = ";
        public const string InsertRoomError = "An exception occurred while inserting room data --> ";
        public const string UpdateRoomError = "An exception occurred while updating room data --> ";
        public const string RoomImageDeleteError = "An exception occurred while deleting room pictures --> ";
        public const string DeleteRoomError = "An exception occurred while deleting room --> ";

        public const string GetActivityDataError = "An exception occurred while get all data from Activity --> ";
        public const string FindActivityByIdError = "An exception occurred while get activity data with Id = ";
        public const string InsertActivityError = "An exception occurred while inserting activity data --> ";
        public const string UpdateActivityError = "An exception occurred while updating activity data --> ";
        public const string ActivityImageDeleteError = "An exception occurred while deleting actvity pictures --> ";
        public const string DeleteActivityError = "An exception occurred while deleting activity --> ";

        public const string InvalidIdError = "Invalid Id Passed";
        public const string EmptyDataSetError = "Empty data array passed";
    }
}