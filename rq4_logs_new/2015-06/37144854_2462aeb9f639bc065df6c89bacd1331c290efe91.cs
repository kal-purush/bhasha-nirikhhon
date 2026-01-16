using System;
using System.Text;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DesignPatterns.BehavioralPatterns.NullObject.Persons;

namespace DesignPatterns.Test.TDD.BehavioralPatterns.NullObject
{
    [TestClass]
    public class CustomerFactoryTest
    {
        private CustomerFactory _customerFactory;

        public CustomerFactoryTest()
        {
            this._customerFactory = new CustomerFactory();
        }

        [TestMethod]
        public void Validate_Instance_Of_Customer()
        {
            Person customer = this._customerFactory.GetCustomerBy("Rafael");
            Assert.IsInstanceOfType(customer, typeof(Customer));
        }

        [TestMethod]
        public void Validate_Instance_Of_NullCustomer()
        {
            Person nullCustomer = this._customerFactory.GetCustomerBy("InvalidCustomer");
            Assert.IsInstanceOfType(nullCustomer, typeof(NullCustomer));
        }
    }
}