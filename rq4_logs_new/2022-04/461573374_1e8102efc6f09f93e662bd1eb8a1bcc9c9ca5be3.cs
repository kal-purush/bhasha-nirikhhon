using System;
using System.Collections.Generic;
using System.Text;

namespace Office
{
    abstract class Employee
    {
        public string TaxId { get; } = GetTaxId();
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public Employee(string firstName, string lastName)
        {
            FirstName = firstName;
            LastName = lastName;
        }

        static int taxIdCounter = 0;
        public static string GetTaxId()
        {
            taxIdCounter++;
            return taxIdCounter.ToString();
        }

        public abstract void Work();
        public abstract void PrintEmployeeInfo();
    }
}