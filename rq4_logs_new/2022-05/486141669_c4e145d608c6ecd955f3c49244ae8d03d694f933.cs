using Xunit;


namespace XUnitExample.Tests
{
    [Collection("Customer")]
    public class CustomerDetailsTests
    {
        private readonly CustomerFixture _customerFixture;

        public CustomerDetailsTests(CustomerFixture customerFixture)
        {
            _customerFixture = customerFixture;
        }

        [Fact]
        public void GetFullName_GivenFisrtAndLastName_ReturnsFullName()
        {
            var customer = _customerFixture.Cust;
            Assert.Equal("Andre Almeida",customer.GetFullName("Andre","Almeida"));
        }  
    }
}