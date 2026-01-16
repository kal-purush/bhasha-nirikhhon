namespace HotelReservation
{
    public class DataContainer
    {
        public UID AccountID = new();
        public static List<Account> Accounts = new();
        public static Dictionary<UID, Hotel> Hotels = new();        // Key is ID of Hotel
        public Dictionary<UID, RoomCategorization> Rooms = new();   // Key is ID of Hotel
        public Dictionary<UID, ReviewGroup> Reviews = new();        // Key is ID of Hotel
        public Dictionary<UID, Fare> Fares = new();                 // Key is ID of RoomGroup
        public Dictionary<UID, Voucher> Vouchers = new();           // Key is ID of Voucher
    }
}