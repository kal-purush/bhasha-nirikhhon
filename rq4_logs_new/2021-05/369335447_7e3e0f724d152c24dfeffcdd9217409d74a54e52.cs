using System;
using Xunit;
using ACM.BL;
using ACM.BL.Repositories;
using System.Collections.Generic;

namespace Acme.BLTest.RepositoryTests
{
    public class ProductRepositoryTest
    {
        [Fact]
        public void RetrieveValid()
        {
            // Arrange
            var repo = new ProductRepository();
            var expected = new Product(2)
            {
                CurrentPrice = 20.43M,
                ProductDescription = "silky smooth",
                ProductName = "Shoe Laces"
            };

            // Act
            var actual = repo.Retrieve(2);

            // Assert
            Assert.True(expected.CurrentPrice == actual.CurrentPrice);
            Assert.True(expected.ProductDescription == actual.ProductDescription);
            Assert.True(expected.ProductName == actual.ProductName);
        }
    }
}