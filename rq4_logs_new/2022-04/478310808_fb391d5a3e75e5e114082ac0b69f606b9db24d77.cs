using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClassLibrary
{
    public class ContactInfo
    {
        //Fields
        private string _streetAddress; 
        private string _city;
        private string _state;
        private string _zip;
        private string _phone;
        private string _email;


        //Properties
        public string StreetAddress { get { return _streetAddress; } set { _streetAddress = value; } }
        public string City { get { return _city; } set { _city = value; } }
        public string State { get { return _state; } set { _state = value; } }
        public string Zip { get { return _zip; } set { _zip = value; } }
        public string Phone { get { return _phone; } set { _phone = value; } }
        public string Email { get { return _email; } set { _email = value; } }

        //Constructors
        public ContactInfo(string streetAddress, string city, string state, string zip, string email, string phone)
        {
            StreetAddress = streetAddress;
            City = city;
            State = state;
            Zip = zip;
            Email = email;
            Phone = phone;
        }

        public ContactInfo()
        {
        }

        //Methods

        public override string ToString()
        {
            return string.Format($"{StreetAddress}\n{City}\n{State}\n{Zip}\n{Email}\n{Phone}");
        }
    }
}