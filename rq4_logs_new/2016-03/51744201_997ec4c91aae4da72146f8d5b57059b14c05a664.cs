using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace WebApplication2
{
    public class Customer
    {
        public int CustID { get; set; }
        public String Name { get; set; }
        public String Address { get; set; }
        public int PhoneNum { get; set; }
        public String Notes { get; set; }

        public String Username { get; set; }
        public String Password { get; set; }

        public Customer(int custID, String name, String address, int phoneNum,
            String notes)
        {
            CustID = custID;
            Name = name;
            Address = address;
            PhoneNum = phoneNum;
            Notes = notes;
        }

        public Customer(String username, String password)
        {
            Password = password;
            Username = username;
        }
    }
}