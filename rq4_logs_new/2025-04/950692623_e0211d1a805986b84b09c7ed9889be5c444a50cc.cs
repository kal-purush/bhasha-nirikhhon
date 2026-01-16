namespace HotelReservation
{
    public partial class Function
    {
        public class HotelSearcher
        {
            public static List<UID> Search(string input)
            {
                var normalizedInput = input.Trim().ToLower();

                Func<KeyValuePair<UID, Hotel>, bool> searchCondition = h =>
                       h.Value.Description.Name.ToLower().Contains(normalizedInput)
                    || h.Value.Description.Address.ToLower().Contains(normalizedInput);

                return HotelManager.Hotels.Where(searchCondition).Select(h => h.Key).ToList();
            }
        }
    }
}