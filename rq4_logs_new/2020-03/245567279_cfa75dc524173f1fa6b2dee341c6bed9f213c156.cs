using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AssetDispatchManager.Controllers
{
    public class Address
    {
        #region Private Address Variables
        private int addressID;
        private string buildingName;
        private string addressLine1;
        private string addressLine2;
        private string city;
        private string state;
        private string Zipcode;
        #endregion

        #region Public Address GetterSetters
        public int AddressID { get => addressID; set => addressID = value; }
        public string BuildingName { get => buildingName; set => buildingName = value; }
        public string AddressLine1 { get => addressLine1; set => addressLine1 = value; }
        public string AddressLine2 { get => addressLine2; set => addressLine2 = value; }
        public string City { get => city; set => city = value; }
        public string State { get => state; set => state = value; }
        public string Zipcode1 { get => Zipcode; set => Zipcode = value; }
        #endregion

        #region Public Address Constructors
        public Address(int addressID, string buildingName, string addressLine1, string addressLine2, string city, string state, string zipcode1)
        {
            AddressID = addressID;
            BuildingName = buildingName;
            AddressLine1 = addressLine1;
            AddressLine2 = addressLine2;
            City = city;
            State = state;
            Zipcode1 = zipcode1;
        } 
        #endregion
    }
}