using AutoMapper;
using Microsoft.AspNetCore.Mvc;
using Moq;
using Neiria.Api.Controllers;
using Neiria.Domain.Interfaces;
using Neiria.Domain.Models;
using Neiria.Domain.ViewModels;
using Neiria.Infrastructure.Repositories;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Neiria.Tests
{
  public class User_CRUD_Tests
  {
    private readonly Mock<IUserRepo> _userRepo;
    private readonly UserController _userController;


    public User_CRUD_Tests()
    {
      _userRepo = new Mock<IUserRepo>();
      _userController = new UserController(_userRepo.Object);
    }
    [Fact]
   
    public async void Test_Read_GetAllUsers_Returns_AllUsers()
    {
      // Arrange
      var mockRepo = new Mock<IUserRepo>();

      mockRepo.Setup(method => method.GetAll()).ReturnsAsync(GetTestUsers());

      var usercontroller = new UserController(mockRepo.Object);
      // Act
      var result = await usercontroller.GetAllUsers();

      // Assert
      var actionResult = Assert.IsType<OkObjectResult>(result.Result);

      var model = Assert.IsAssignableFrom<IEnumerable<User>>(actionResult.Value);

      Assert.Equal(3, model.Count());
    }

    [Fact]
    public async void Test_Read_GetUserbyId_Returns_UserValid()
    {
      // Arrange
      var mockRepo = new Mock<IUserRepo>();
      var user = new User()
      {
        Guid = new Guid("fb2507a9-8175-4c0f-8fa8-7a8a02cb4780"),
        Name = "Tom",
        LastName = "Riddle",
        Email = "TheDarkLord@gmail.com",
        Age = 32
      };

      mockRepo.Setup(method => method.GetId(user.Guid)).ReturnsAsync(GetTestUser());                                                                                                                                                                                                                                                                                                                                                                                                                                

      // Act
      var usercontroller = new UserController(mockRepo.Object);
  
      var result = await usercontroller.GetSpecificUser(new Guid("fb2507a9-8175-4c0f-8fa8-7a8a02cb4780"));
      //Assert
      var actionResult = Assert.IsType<OkObjectResult>(result.Result);
      var model = Assert.IsAssignableFrom<User>(actionResult.Value);
      Assert.Equal(user.Guid, model.Guid);
      Assert.Equal(user.Name, model.Name);
      Assert.Equal(user.Age, model.Age);
    }

    [Fact]
    public async Task Test_Create_Insert_ReturnsValid()
    {

      // Arrange
      var mockRepo = new Mock<IUserRepo>();

    }


    private static IEnumerable<User> GetTestUsers()
    {
      var users = new List<User>();
      users.Add(new User()
      {
        Guid = new Guid("2579eb7d-637b-4a6d-b827-d6e9d4471d7f"),
        Name = "James",
        LastName = "Potter",
        Email = "JamesPotter@gmail.com",
        Age = 45
      });
      users.Add(new User()
      {
        Guid = new Guid("fb2507a9-8175-4c0f-8fa8-7a8a02cb4780"),
        Name = "Tom",
        LastName = "Riddle",
        Email = "TheDarkLord@gmail.com",
        Age = 32
      });
      users.Add(new User()
      {
        Guid = new Guid("bc8484b0-7e24-4b7e-aadd-fe100e7ec7bd"),
        Name = "Jar Jar",
        LastName = "Binks",
        Email = "DarkSith@gmail.com",
        Age = 22
      });
      return users;
    }

    private User GetTestUser()
    {
      var user = new User()
      {
        Guid = new Guid("fb2507a9-8175-4c0f-8fa8-7a8a02cb4780"),
        Name = "Tom",
        LastName = "Riddle",
        Email = "TheDarkLord@gmail.com",
        Age = 32
      };
      return user;
    }
  }
}