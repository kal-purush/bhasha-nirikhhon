using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Darkec.Models
{
    public class Order
    {
        public static User User { get; set; }
        public static Dictionary<int, Apartment> ApartmentDictionary = new Dictionary<int, Apartment>();
        public static Dictionary<int, Boat> BoatDictionary = new Dictionary<int, Boat>();

        public static void AddApartmentToOrder(Apartment apartment)
        {
            ApartmentDictionary.Add(apartment.Id, apartment);
        }
        public static void AddBoatToOrder(Boat boat)
        {
            BoatDictionary.Add(boat.Id, boat);
        }
        public static void RemoveApartmentFromOrder(Apartment apartment)
        {
            ApartmentDictionary.Remove(apartment.Id);
        }
        public static void RemoveBoatFromOrder(Boat boat)
        {
            BoatDictionary.Remove(boat.Id);
        }
    }
}