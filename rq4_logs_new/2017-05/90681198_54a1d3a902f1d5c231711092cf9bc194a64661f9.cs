using System;
using BangazonSiteMVC.Controllers;
using BangazonSiteMVC.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;

namespace BangazonSiteMVC.Tests
{
    [TestClass]
    public class CustomerTests
    {
        [TestMethod]
        public void EnsureAddCustomer()
        {
            //Arrange
            var customerRepository = new Mock<ICustomerRepository>();
            var controller = new CustomerController(customerRepository.Object);
            var newCustomer = new Customer
            {
                Id = 1,
                FullName = "Josh Kilgore"
            };

            //Act

            //Assert
        }
    }
}