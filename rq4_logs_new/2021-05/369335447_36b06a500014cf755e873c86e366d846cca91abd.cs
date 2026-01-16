using System;
using System.Security.Cryptography.X509Certificates;
using ACM.BL;
using Xunit;
namespace Acme.BLTest
{
    public class ProductTest
    {
        [Fact]
        public void ValidateValid()
        {
            // Arrange
            Product product = new Product
            {
                CurrentPrice = 14,
                ProductName = "Banana Milk"
            };
            bool expected = true;

            // Act
            bool actual = product.Validate();

            // Assert
            Assert.True(expected == actual);
        }

        [Fact]
        public void ValidateMissingName()
        {
            // Arrange
            Product mystery = new Product
            {
                CurrentPrice = 30
            };
            bool expected = false;

            // Act
            bool actual = mystery.Validate();

            // Assert
            Assert.True(expected == actual);
        }

        [Fact]
        public void ValidateMissingPrice()
        {
            // Arrange
            Product freebie = new Product
            {
                ProductName = "freebie!"
            };
            bool expected = false;

            // Act
            bool actual = freebie.Validate();

            // Assert
            Assert.True(expected == actual);
        }

        //[Fact]
        //public void XXX()
        //{
        //    // Arrange

        //    string expected;

        //    // Act
        //    string actual;

        //    // Assert
        //    Assert.True(expected == actual);
        //}


    }
}