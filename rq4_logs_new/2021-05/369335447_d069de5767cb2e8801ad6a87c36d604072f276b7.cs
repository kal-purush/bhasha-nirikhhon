using System;
using Xunit;
using System.Collections.Generic;
using Acme.Common;
using ACM.BL;

namespace Acme.CommonTests
{
    public class LoggingServiceTest
    {
        [Fact]
        public void WriteToFileTest()
        {
            // Arrange
            var changedItems = new List<ILoggable>();
            var customer = new Customer()
            {
                Email = "foo@bar.org",
                FirstName = "Foo",
                LastName = "Bar",
                AddressList = null
            };
            changedItems.Add(customer);

            var product = new Product
            {
                ProductName = "duct tape",
                ProductDescription = "the reaaaallll sticky stuff",
                CurrentPrice = 6m
            };
            changedItems.Add(product);

            // Act
            LoggingService.WriteToFile(changedItems);

            // Assert
            // Nothing to assert at this time. 
        }
    }
}