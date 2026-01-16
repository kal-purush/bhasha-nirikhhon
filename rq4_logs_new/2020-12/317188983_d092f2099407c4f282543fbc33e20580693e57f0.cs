using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Darkec.Models;
using Darkec.Services;

namespace Darkec.MockData
{
    public class MockApartments : IApartmentRepository
    {
        private Dictionary<int, Apartment> apartments { get; }
        private static MockApartments _instance;
        private MockApartments()
        {
            apartments = new Dictionary<int, Apartment>();
            apartments.Add(1, new Apartment() { Id = 1, Name = "AP1", Description = "One room, one toilet, kitchen and balcony", Price = 35 , ImageName= "Cheese_pizza.jfif" });
            apartments.Add(2, new Apartment() { Id = 2, Name = "AP2", Description = " Maden of bufalla", Price = 59, ImageName = "Bufalla_pizza.jfif" });
            apartments.Add(3, new Apartment() { Id = 3, Name = "AP3", Description = " Maden of chicken", Price = 120, ImageName = "Chicken_pizza.jfif" });
        }

        public static MockApartments Instance
        {
            get
            {
                if (_instance == null)
                {
                    _instance = new MockApartments();
                }
                return _instance;
            }
        }

        public Dictionary<int,Apartment> AllApartments()
        {
            return apartments;
        }
        public void AddApartment(Apartment apartment)
        {
            if (!(apartments.Keys.Contains(apartment.Id)))
            {
                apartments.Add(apartment.Id, apartment);
            }
                
        }
        
        public Apartment GetApartment(int id)
        {
            return apartments[id];
        }

        public void UpdateApartment(Apartment apartment)
        {
            if (apartment != null)
            {
                apartments[apartment.Id] = apartment;
            }
        }

        public void DeleteApartment(Apartment apartment)
        {
            if (apartment != null)
            {
                apartments.Remove(apartment.Id, out apartment);
            }
        }

        public Dictionary<int, Apartment> FilterApartment(string apartmentName)
        {
            
            Dictionary<int, Apartment> filteredApartments = new Dictionary<int, Apartment>();

            foreach (Apartment apartment in apartments.Values)
            {
                if (apartment.Name.StartsWith(apartmentName))
                {
                    filteredApartments.Add(apartment.Id, apartment);
                }
            }
            return filteredApartments;
        }
    }
}