using ConsoleApp54.Entities;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsoleApp54.Abstract
{
    public abstract class BaseCustomerManager : IPersonCheckService
    {
        public bool CheckIfRealPerson(Customer customer)
        {
            throw new NotImplementedException();
        }

        public virtual void Save(Customer customer)
        {
            Console.WriteLine("Saved to db: "+customer.FirstName) ;
        }

        internal void Save(Customer customer, object p)
        {
            throw new NotImplementedException();
        }
    }
}