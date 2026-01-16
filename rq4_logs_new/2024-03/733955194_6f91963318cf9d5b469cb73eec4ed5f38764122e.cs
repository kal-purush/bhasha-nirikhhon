using FluentAssertions;
using KlockanAPI.Application.CrossCutting;
using KlockanAPI.Application.DTOs.ClassroomUser;
using KlockanAPI.Application.Services;
using KlockanAPI.Domain.Models;
using KlockanAPI.Infrastructure.Repositories.Interfaces;
using MapsterMapper;
using NSubstitute;

namespace KlockanAPI.Application.Tests.Services;

public class ClassroomUserServiceTests
{
    private readonly IClassroomUserRepository _classroomUserRepository;
    private readonly IClassroomRepository _classroomRepository;
    private readonly IMapper _mapper;

    public ClassroomUserServiceTests()
    {
        _classroomRepository = Substitute.For<IClassroomRepository>();
        _classroomUserRepository = Substitute.For<IClassroomUserRepository>();
        _mapper = new Mapper();
    }


    public ClassroomUserService GetServiceInstance() => new(_classroomUserRepository, _classroomRepository, _mapper);

    [Fact]
    public async Task GetUsersByClassroomIdAsync_ShouldThrowNotFoundError_WhenClassroomIdIsInvalid()
    {
        // Arrage
        var classroomId = 5;
        var classroomUserService = GetServiceInstance();

        // Act
        var act = async () => await classroomUserService.GetUsersByClassroomIdAsync(classroomId);

        // Assert
        await act.Should()
            .ThrowAsync<NotFoundException>()
            .WithMessage($"Classroom with id {classroomId} not found");
    }

    [Fact]
    public async Task GetUsersByClassroomIdAsync_ShouldReturnClassroomUserDTOs()
    {
        // Arrage
        var classroomUserService = GetServiceInstance();

        var classroom = new Classroom() { Id = 5 };
        var usersSample = new List<ClassroomUser>() {
            new ClassroomUser() { UserId = 1, ClassroomId = classroom.Id, RoleId = 2 },
            new ClassroomUser() { UserId = 2, ClassroomId = classroom.Id, RoleId = 3 },
        };

        _classroomRepository.GetClassroomByIdAsync(classroom.Id).Returns(Task.FromResult<Classroom?>(classroom));
        _classroomUserRepository.GetUsersByClassroomIdAsync(classroom.Id)
            .Returns(Task.FromResult<IEnumerable<ClassroomUser>>(usersSample));

        // Act
        var result = await classroomUserService.GetUsersByClassroomIdAsync(classroom.Id);

        // Assert
        result.Should().NotBeNull();
        result.Should().HaveCount(usersSample.Count);
        result.Should().BeEquivalentTo(_mapper.Map<List<ClassroomUserDTO>>(usersSample));
    }
}