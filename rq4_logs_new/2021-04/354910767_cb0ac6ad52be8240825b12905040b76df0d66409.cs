using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using NSubstitute;
using Studio_Inventory_API.Controllers;
using Studio_Inventory_API.Models;
using Studio_Inventory_API.Repositories;

namespace Studio_Inventory_API.Tests
{
    public class UserControllerTests
    {

        UserController sut;
        IRepository<User> userRepo;

        public UserControllerTests()
        {
            userRepo = Substitute.For<IRepository<User>>();
            sut = new UserController(userRepo);
        }

        [Fact]
        public void GetUsers_Returns_A_List()
        {
            IEnumerable<User> expectedList = new List<User>();
            userRepo.GetAll().Returns(expectedList);

            var result = sut.GetUsers();

            Assert.Equal(expectedList, result);
        }

        [Fact]
        public void GetUsers_Returns_A_User()
        {
            var expectedUser = new User();
            userRepo.GetById(1).Returns(expectedUser);

            var result = sut.GetUser(1);

            Assert.Equal(expectedUser, result);
        }


    }
}