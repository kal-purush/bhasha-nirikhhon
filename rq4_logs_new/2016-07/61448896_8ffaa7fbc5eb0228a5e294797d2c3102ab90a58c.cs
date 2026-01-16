namespace TeamsSuperMan
{
    using System.Collections.Generic;
    public class CityTransportCardsList : List<ICartable>
    {
        public CityTransportCardsList()
        {
            Add(new CityTransportCart("One days card for all transports", 4));
            Add(new CityTransportCart("One days card for metro", 3));
            Add(new CityTransportCart("Three days card for all transports", 10));
            Add(new CityTransportCart("Three days card for metro", 10));
        }

        public List<ICartable> GetCarts()
        {
            return this;
        }
    }
}