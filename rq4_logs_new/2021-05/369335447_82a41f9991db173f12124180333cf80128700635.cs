using System;
using ACM.BL;
using Xunit;
namespace Acme.BLTest
{
    public class OrderItemTest
    {
        [Fact]
        public void ValidateValid()
        {
            // Arrange
            OrderItem orderItem = new OrderItem
            {
                PurchasePrice = 42,
                ProductId = 3,
                Quantity = 2
            };
            bool expected = true;

            // Act
            bool actual = orderItem.Validate();

            // Assert
            Assert.True(expected == actual);
        }

        [Fact]
        public void ValidateMissingQuantity()
        {
            //Arrange
            OrderItem item = new OrderItem
            {
                PurchasePrice = 42,
                ProductId = 2
            };
            bool expected = false;

            // Act
            bool actual = item.Validate();

            // Assert
            Assert.True(expected == actual);
        }

        [Fact]
        public void ValidateProductIdZero()
        {
            //Arrange
            OrderItem item = new OrderItem
            {
                PurchasePrice = 42,
                Quantity = 99,
                ProductId = 0
            };
            bool expected = false;

            // Act
            bool actual = item.Validate();

            // Assert
            Assert.True(expected == actual);
        }

        [Fact]
        public void ValidateMissingPrice()
        {
            //Arrange
            OrderItem item = new OrderItem
            {
                ProductId = 6,
                Quantity = 1
            };
            bool expected = false;

            // Act
            bool actual = item.Validate();

            // Assert
            Assert.True(expected == actual);
        }
    }
}