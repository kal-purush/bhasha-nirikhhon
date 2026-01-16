using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlexWheels
{
    internal class Account
    {
        private string name;
        private string contactNumber;
        private int id;
        private string email;
        private string password;

        public string Name
        {
            get { return name; }
            set { name = value; }
        }

        public string ContactNumber
        {
            get { return contactNumber; }
            set { contactNumber = value; }
        }

        public int Id
        {
            get { return id; }
            set { id = value; }
        }

        public string Email
        {
            get { return email; }
            set { email = value; }
        }

        public string Password
        {
            get { return password; }
            set { password = value; }
        }

        public Account() { }

        public Account(string n, string cn, int i, string e, string pass)
        {
            name = n;
            contactNumber = cn;
            id = i;
            email = e;
            password = pass;
        }

        public override string ToString()
        {
            return "Name: " + Name + "\nContact Number: " + ContactNumber + "\nID: " + Id + "\nEmail: " + Email + "\nPassword: " + Password;
        }
    }
}