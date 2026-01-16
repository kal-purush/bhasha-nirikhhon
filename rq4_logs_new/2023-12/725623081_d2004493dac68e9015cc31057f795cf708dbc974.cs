using Backend.Controllers;
using Backend.DataAccessObjects.User;
using Microsoft.AspNetCore.Mvc;
using Moq;
using Shared;

namespace TestProjectAPI;


[TestFixture]
public class UserControllerTests
{
    [Test]
    public async Task CreateUserAccountAsync_ReturnsUserId()
    {
        // Arrange
        var userInterfaceMock = new Mock<IUserInterface>();
        var userController = new UserController(userInterfaceMock.Object);
        var newUser = new User { Mail="khaled@gmail.com" ,Password = "123456", Username = "KhaledBackend"};
        var expectedUserId = 123456; 

        userInterfaceMock
            .Setup(x => x.CreateUserAccountAsync(newUser))
            .ReturnsAsync(expectedUserId);

        // Act
        var result = await userController.CreateUserAccountAsync(newUser);

        // Assert
        Assert.IsInstanceOf<ActionResult<long>>(result);
        var objectResult = (ObjectResult)result.Result!;
        Assert.AreEqual(200, objectResult.StatusCode);
        Assert.AreEqual(expectedUserId, (long)(objectResult.Value ?? 0));
    }

    [Test]
    public async Task CreateUserAccountAsync_ExceptionThrown_ReturnsInternalServerError()
    {
        // Arrange
        var userInterfaceMock = new Mock<IUserInterface>();
        var userController = new UserController(userInterfaceMock.Object);
        var invalidUser = new User { Id=1234,Mail="khaled@gmail.com" ,Password = "123456", Username = "KhaledBackend" };

        userInterfaceMock
            .Setup(x => x.CreateUserAccountAsync(invalidUser))
            .ThrowsAsync(new Exception("Some error message"));

        // Act
        var result = await userController.CreateUserAccountAsync(invalidUser);

        // Assert
        Assert.IsInstanceOf<ActionResult<long>>(result);
        var objectResult = (ObjectResult)result.Result!;
        Assert.AreEqual(500, objectResult.StatusCode);
        Assert.AreEqual("Some error message", objectResult.Value);
    }
    
}