using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BEBourbonCollective.Interfaces;
using BEBourbonCollective.Models;
using BEBourbonCollective.Services;
using Moq;


namespace BEBourbonCollective.Tests
{
    public class UserTests
    {
        private readonly UserService _userService;

        private readonly Mock<IUserRepository> _mockUserRepository;

        public UserTests()
        {
            _mockUserRepository = new Mock<IUserRepository> ();
            _userService = new UserService(_mockUserRepository.Object);
        }

        [Fact]

        public async Task GetAllUsersAsync_ShouldReturnListOfUsers()
        {
            // Arrange
            var users = new List<User>
            {
                new User { Id = 1, FullName="John Doe"},
                new User { Id = 2, FullName="Jane Doe"}
            };

            // Setup
            _mockUserRepository.Setup(repo => repo.GetAllUsersAsync()).ReturnsAsync(users);

            // Act
            var result = await _userService.GetAllUsersAsync();

            // Assert
            _mockUserRepository.Verify(repo => repo.GetAllUsersAsync(), Times.Once());
            Assert.Equal(users.Count, result.Count);
            Assert.Equal(users[0].Id, result[0].Id);
            Assert.Equal(users[1].Id, result[1].Id);

        }

        [Fact]
        public async Task RegisterUserAsync_ShouldReturnNewUserId()
        {
            // Arrange
            var newUser = new User
            {
                Id = 6,
                FirebaseId = "C0wunKp1sIQRM9YR48JnQPlNXt95",
                Username = "newUser01",
                FullName = "New User",
                Email = "newuser@gmail.com",
                City = "Lexington",
                State = "Kentucky"
            };

            //Setup
            _mockUserRepository
                .Setup(repo => repo.RegisterUserAsync(newUser))
                .ReturnsAsync(newUser);

            // Act
            var actualUser = await _userService.RegisterUserAsync(newUser);

            // Assert
            Assert.Equal(newUser.Id, actualUser.Id);
        }

        [Fact]
        public async Task UpdateSingleUserAsync_ShouldReturnUpdatedPlaylist_WhenPlaylistExists()
        {
            // Arrange
            var updatedUser = new User
            {
                Id = 6,
                FirebaseId = "C0wunKp1sIQRM9YR48JnQPlNXt95",
                Username = "updatedUser01",
                FullName = "Updated User",
                Email = "updateduser@gmail.com",
                City = "Lexington",
                State = "Kentucky"
            };

            // Setup
            _mockUserRepository
                .Setup(repo => repo.UpdateSingleUserAsync(updatedUser.Id, updatedUser))
                .ReturnsAsync(updatedUser);

            // Act
            var result = await _userService.UpdateSingleUserAsync(updatedUser.Id, updatedUser);

            // Assert
            Assert.Equal(updatedUser, result);
        }
    }
}