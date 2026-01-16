using System;
using System.Linq;
using AutoMapper;
using Microsoft.AspNetCore.Mvc;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using NeutrackAPI.Controllers;
using NeutrackAPI.Data;
using NeutrackAPI.Data.IRepositories;
using NeutrackAPI.Models;
using NeutrackAPI.tests.Fixtures;

namespace NeutrackAPI.tests.Controller.Tests
{
    /// <summary>
    /// Unit test for user controller
    /// </summary>
    [TestClass]
    public class UserControllerUnitTest
    {
        private Mock<IUserRepository> mockUserRepository;
        private Mock<IRoleRepository> mockRoleRepository;
        private Mock<IMapper> mockMapper;
        private IQueryable<User> mockUsersData;
        private User mockUser;

        /// <summary>
        /// Initializes and sets up mock before each tests run
        /// </summary>
        [TestInitialize]
        public void SetUp()
        {
            mockUserRepository = new Mock<IUserRepository>();
            mockRoleRepository = new Mock<IRoleRepository>();
            mockMapper = new Mock<IMapper>();
            mockUsersData = GenerateUser.GenerateListData(3).AsQueryable();
            mockUser = GenerateUser.GeneratData();
        }

        // <summary>
        /// Test should return a action result 200
        /// </summary>
        [TestMethod]
        [TestCategory("Controller Test")]
        public void ActionResultGetAllUsersShouldReturnAllUsers()
        {
            mockUserRepository.Setup(x => x.GetAllUsers()).Returns(mockUsersData);
            UsersController testUserController = new UsersController(mockUserRepository.Object,
                mockRoleRepository.Object, mockMapper.Object);
            var allUsers = testUserController.GetUsers();
            Assert.IsInstanceOfType(allUsers.Result, typeof(OkObjectResult));
        }

        // <summary>
        /// Test should return a action result 200
        /// </summary>
        [TestMethod]
        [TestCategory("Controller Test")]
        public void ActionResultGetAUserByIdShouldReturnAUsers()
        {
            int userId = 1;
            mockUserRepository.Setup(x => x.GetUserById(userId)).Returns(mockUser);
            UsersController testUserController = new UsersController(mockUserRepository.Object,
                mockRoleRepository.Object, mockMapper.Object);
            var user = testUserController.GetUserById(userId);
            Assert.IsInstanceOfType(user.Result, typeof(OkObjectResult));
        }

    }
}