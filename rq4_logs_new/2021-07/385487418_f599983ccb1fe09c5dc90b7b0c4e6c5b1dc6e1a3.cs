using System;

namespace Persons
{
    public class Employee : Person
    {
        private string _jobTitle;

        public Employee()
        {
            this._id = ++_maximumId;
        }

        public Employee(string jobTitle, string firstName, string lastName, string address)
        {
            this._id = ++_maximumId;
            this._jobTitle = jobTitle;
            this._firstName = firstName;
            this._lastName = lastName;
            this._address = address;
        }

        public string JobTitle {
            get { return _jobTitle; }
            set { _jobTitle = value; }
        }

        public override void Display()
        {
            Console.WriteLine($"Employee {_firstName} {_lastName} (address: {_address}) has a position of {_jobTitle}");
        }
    }
}