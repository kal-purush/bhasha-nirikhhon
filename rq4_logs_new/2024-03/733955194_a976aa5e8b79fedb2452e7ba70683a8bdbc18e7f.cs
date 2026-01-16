using NSubstitute;
using FluentAssertions;
using Microsoft.AspNetCore.Mvc;

using KlockanAPI.Application.Services.Interfaces;
using KlockanAPI.Presentation.Controllers;
using KlockanAPI.Application.DTOs.Schedule;
using KlockanAPI.Application.DTOs.ClassroomUser;

namespace KlockanAPI.Presentation.Tests.Controllers;

public class ClassroomUsersControllerTests
{
    private readonly IClassroomUserService _classroomUserService;

    public ClassroomUsersControllerTests()
    {
        _classroomUserService = Substitute.For<IClassroomUserService>();
    }

    private ClassroomUsersController GetControllerInstance() => new(_classroomUserService);

    [Fact]
    public async Task GetUsersByClassroomIdAsync_ShouldReturnOk()
    {
        // Arrange
        var classroomUsersController = GetControllerInstance();

        List<ClassroomUserDTO> classroomUsersDTOs = new List<ClassroomUserDTO>
        {
            new ClassroomUserDTO() { UserId = 1, ClassroomId = 1, RoleId = 2 },
            new ClassroomUserDTO() { UserId = 2, ClassroomId = 1, RoleId = 3 },
            new ClassroomUserDTO() { UserId = 3, ClassroomId = 1, RoleId = 4 },
        };
        _classroomUserService.GetUsersByClassroomIdAsync(1).Returns(classroomUsersDTOs);

        // Act
        var result = await classroomUsersController.GetClassroomUsersByClassroomId(1);

        // Assert
        result.Should().BeOfType<ActionResult<List<ScheduleDTO>>>();
        result.Result.Should().BeOfType<OkObjectResult>();
        (result?.Result as OkObjectResult)?.StatusCode.Should().Be(200);
    }
}