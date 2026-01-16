using System;
using System.Collections.Generic;
using System.Text;

namespace ConsumerPayments
{
    abstract class Consumer
    {
        public string Payment { get; }
        public string FirstName { get; set; }
        public string LastName { get; set; }

        public Consumer(string firstName, string lastName, string payment)
        {
            FirstName = firstName;
            LastName = lastName;
            Payment = payment;
        }

        public abstract void Pay();
    }
}