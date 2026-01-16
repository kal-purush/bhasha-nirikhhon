using System;
using Xunit;
using ACM.BL;
using ACM.BL.Repositories;

namespace Acme.BLTest.RepositoryTests
{
    public class CustomerRepositoryTest
    {
        [Fact]
        public void RetrieveValid()
        {
            // Arrange
            var repo = new CustomerRepository();
            var expected = new Customer(1)
            {
                Email = "Foo@bar.org",
                FirstName = "Foo",
                LastName = "Bar"
            };
            // Act
            var actual = repo.Retrieve(1);

            // Assert
            Assert.True(expected.FirstName == actual.FirstName);
            Assert.True(expected.LastName == actual.LastName);
            Assert.True(expected.Email == actual.Email);
            Assert.True(expected.CustomerId == actual.CustomerId);
        }
    }
}