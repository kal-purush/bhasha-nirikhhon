using System;
using Xunit;
using ACM.BL;
using ACM.BL.Repositories;

namespace Acme.BLTest.RepositoryTests
{
    public class OrderRepositoryTest
    {
        [Fact]
        public void RetrieveValid()
        {
            // Arrange
            var repo = new OrderRepository();
            var expected = new Order(10)
            {
                OrderDate = new DateTimeOffset(DateTime.Now.Year, 4, 14, 10, 00, 00, new TimeSpan(7, 0, 0))
            };

            // Act
            var actual = repo.Retrieve(10);

            // Assert
            Assert.True(expected.OrderDate == actual.OrderDate);
        }
    }
}