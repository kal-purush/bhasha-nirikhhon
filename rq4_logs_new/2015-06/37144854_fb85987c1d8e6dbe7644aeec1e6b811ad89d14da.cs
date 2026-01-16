using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DesignPatterns.BehavioralPatterns.NullObject.Persons
{
    public class CustomerFactory
    {
        private List<string> _customersName;

        public CustomerFactory()
        {
            this._customersName = new List<string>() { "Rafael", "Oliveira", "Benetti"};
        }

        public Person GetCustomerBy(string name)
        {
            if (this._customersName.Contains(name))
            {
                return new Customer(name);
            }

            return new NullCustomer();
        }
    }
}