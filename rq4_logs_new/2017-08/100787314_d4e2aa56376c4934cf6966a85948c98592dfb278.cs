using System;
using System.Collections.Generic;
using System.Linq;
using CustomerAppBLL;
using CustomerAppBLL.Services;
using CustomerAppDAL;
using CustomerAppDAL.Repositories;
using CustomerAppEntity;
using Xunit;

namespace CustomerAppBLLTests
{
    public class CustomerServiceTests
    {
        private static readonly Customer MockCustomer = new Customer()
        {
            FirstName = "Test",
            LastName = "Testesen",
            Address = "Secret"
        };

        private readonly IService<Customer> _customerService;

        public CustomerServiceTests()
        {
            IRepository<Customer> mockRepo = new MockCustomerRepository();
            _customerService = new CustomerService(mockRepo);
        }

        [Fact]
        public void TestGetAll()
        {
            var customers = _customerService.GetAll();

            Assert.NotNull(customers);
        }

        [Fact]
        public void TestGetById()
        {
            var createdCustomer = _customerService.Create(MockCustomer);

            var customerFromSearch = _customerService.GetById(createdCustomer.Id);

            Assert.Equal(createdCustomer, customerFromSearch);
        }

        [Fact]
        public void TestCreateSuccess()
        {
            var createdCustomer = _customerService.Create(MockCustomer);

            Assert.NotNull(createdCustomer);
        }

        [Fact]
        public void TestCreateDuplicateFail()
        {
            var created1 = _customerService.Create(MockCustomer);
            var created2 = _customerService.Create(MockCustomer);

            Assert.Null(created2);
        }

        [Fact]
        public void TestCreateEmptyFail()
        {
            var newCustomer = new Customer();

            var createdCustomer = _customerService.Create(newCustomer);

            Assert.Null(createdCustomer);
        }


        [Fact]
        public void TestDeleteSuccess()
        {
            var newPerson = _customerService.Create(MockCustomer);

            var deleteSucceeded = _customerService.Delete(newPerson.Id);

            Assert.True(deleteSucceeded);
        }
    }
}