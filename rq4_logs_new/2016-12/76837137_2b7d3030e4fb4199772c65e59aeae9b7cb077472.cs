using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace RandomPatientGenerator.Model
{

    public enum Gender
    {
        Male = 0,
        Female = 1
    };

    public class Patient
    {
        public string MRN { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public Gender PatientGender { get; set; }
        public DateTime DOB { get; set; }
        public Address CurrentAddress { get; set; }

        public Patient(string thisMRN, string firstName, string lastName, string dateOfBirth)
        {
            MRN = thisMRN;
            FirstName = firstName;
            LastName = lastName;
            DOB = DateTime.Parse(dateOfBirth);
        }

        public Patient(string thisMRN, string firstName, string lastName, string dateOfBirth, Address setAddress) : this(thisMRN, firstName, lastName, dateOfBirth)
        {
            CurrentAddress = setAddress;
        }

        public Patient(string thisMRN, string firstName, string lastName, string dateOfBirth, string addressLine1, string addressLine2, string city, string state, string postalCode) : this(thisMRN, firstName, lastName, dateOfBirth)
        {
            this.CurrentAddress = new Address(addressLine1, addressLine2, city, state, postalCode);
        }

        public string viewPatientRecord()
        {
            return String.Format("{0},{1},{2},{3},{4:yyyy/MM/dd},{5}", MRN, LastName, FirstName, PatientGender, DOB, CurrentAddress.ToString());
        }
    }

    public class Address
    {
        public string AddressStreet1 { get; set; }
        public string AddressStreet2 { get; set; }
        public string City { get; set; }
        public string State { get; set; }
        public string PostalCode { get; set; }

        public Address(string addressLine1, string addressLine2, string city, string state, string postalCode)
        {
            AddressStreet1 = addressLine1;
            AddressStreet2 = addressLine2;
            City = city;
            State = state;
            PostalCode = postalCode;
        }

        public override string ToString()
        {
            return String.Format("{0},{1},{2},{3},{4}", AddressStreet1, AddressStreet2, City, State, PostalCode);
        }
    }
}