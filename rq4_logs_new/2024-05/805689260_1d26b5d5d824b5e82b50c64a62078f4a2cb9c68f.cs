using AutoMapper;
using FakeItEasy;
using FluentAssertions;
using FreelanceApp.Controllers;
using FreelanceApp.DTOs;
using FreelanceApp.Models;
using FreelanceApp.Mappers;
using FreelanceApp.Interfaces;
using FreelanceApp.Repository;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FreelanceApp.Tests.Controller
{
    
    public class UserControllerTests
    {
        private readonly IUserRepository _userRepository;
        private readonly IMapper _mapper;
        private readonly UserController _userController;
        public UserControllerTests()
        {
            _userRepository = A.Fake<IUserRepository>();
            _mapper = A.Fake<IMapper>();
            _userController = new UserController(_userRepository, _mapper);
        }

        [Fact]
        public void UserController_GetUsers_ReturnOk()
        {
            //Arrange
            var users = A.Fake<ICollection<UserDto>>();
            var userList = A.Fake<List<UserDto>>();
            A.CallTo(() => _mapper.Map<List<UserDto>>(users)).Returns(userList);
            var controller = new UserController(_userRepository, _mapper);

            //Act
            var query = new Helpers.QueryObject();
            var result = controller.GetUsers(query);
            //Assert
            result.Should().NotBeNull();
            result.Should().BeOfType(typeof(OkObjectResult));

        }

        [Fact]
        public void UserController_CreateUser_ReturnsCreatedAtAction()
        {
            // Arrange
            var userCreateDto = new CreateUserDto
            {
                Username = "testuser",
                Email = "test@example.com",
                PhoneNumber = "1234567890",
                Skillsets = "C#, ASP.NET",
                Hobby = "Reading"
            };

            var user = new User
            {
                Id = 1,
                Username = userCreateDto.Username,
                Email = userCreateDto.Email,
                PhoneNumber = userCreateDto.PhoneNumber,
                Skillsets = userCreateDto.Skillsets,
                Hobby = userCreateDto.Hobby
            };

            A.CallTo(() => _userRepository.GetUserByEmail(userCreateDto.Email)).Returns(null);
            A.CallTo(() => _userRepository.GetUserByUsername(userCreateDto.Username)).Returns(null);
            A.CallTo(() => _userRepository.GetUserByPhoneNumber(userCreateDto.PhoneNumber)).Returns(null);
            A.CallTo(() => _mapper.Map<User>(userCreateDto)).Returns(user);
            A.CallTo(() => _userRepository.CreateUser(A<User>.Ignored)).Returns(user);

            var controller = new UserController(_userRepository, _mapper);

            // Act
            var result = controller.CreateUser(userCreateDto);

            // Assert
            result.Should().NotBeNull();
            result.Should().BeOfType<CreatedAtActionResult>();

            var createdAtActionResult = result as CreatedAtActionResult;
            createdAtActionResult.ActionName.Should().Be(nameof(UserController.GetUserById));
            createdAtActionResult.RouteValues["id"].Should().Be(user.Id);
            createdAtActionResult.Value.Should().BeEquivalentTo(user);
        }
    }
}