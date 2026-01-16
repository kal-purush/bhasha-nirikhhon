using Microsoft.EntityFrameworkCore;
using FluentAssertions;

using KlockanAPI.Domain.Models;
using KlockanAPI.Infrastructure.Data;
using KlockanAPI.Infrastructure.Repositories;
using Microsoft.AspNetCore.Mvc.Diagnostics;

namespace KlockanAPI.Infraestructure.Tests.Repositories;

public class ClassroomUserRepositoryTests : IDisposable
{
    private readonly KlockanContext _context;

    public ClassroomUserRepositoryTests()
    {
        DbContextOptionsBuilder<KlockanContext> dbContextOptions = new DbContextOptionsBuilder<KlockanContext>()
            .UseInMemoryDatabase(Guid.NewGuid().ToString());

        _context = new KlockanContext(dbContextOptions.Options);
    }

    private ClassroomUserRepository GetRepositoryInstance() => new(_context);

    public void Dispose()
    {
        _context.Database.EnsureDeleted();
    }

    [Theory]
    [InlineData(1, 0)]
    [InlineData(2, 2)]
    public async Task GetClassroomUsersByClassroomIdAsync_ShouldReturnAListOfClassroomUsers_WhenClassroomIdIsProvided(int classroomId, int classroomUsersCount)
    {
        // Arrage
        var classroomUsersRepository = GetRepositoryInstance();

        var classroomUsers = new List<ClassroomUser>() {
            new ClassroomUser
            {
                Id = 1,
                ClassroomId = 2,
                UserId = 1,
                RoleId = 1
            },
            new ClassroomUser
            {
                Id = 2,
                ClassroomId = 2,
                UserId = 2,
                RoleId = 1
            },
        };

        _context.ClassroomUsers.AddRange(classroomUsers);

        await _context.SaveChangesAsync();

        // Act 
        var result = await classroomUsersRepository.GetUsersByClassroomIdAsync(classroomId);

        // Assert
        result.Count().Should().Be(classroomUsersCount);
    }

    [Fact]
    public async Task UpdateClassroomAsync_ShouldUpdateNestedSchedules_WhenSchedulesChange()
    {
        // Arrange
        var classroomUserRepository = GetRepositoryInstance();

        // Setup database data
        var program = new Program() { Id = 1 };
        var course = new Course() { Id = 1 };
        var classrooms = new List<Classroom>()
        {
            new Classroom() { Id = 1, ProgramId = program.Id, CourseId = course.Id },
            new Classroom() { Id = 2, ProgramId = program.Id, CourseId = course.Id }
        };
        var roles = new List<Role>()
        {
            new Role { Id = 1, Name = "Admin" },
            new Role { Id = 2, Name = "Trainer" },
            new Role { Id = 3, Name = "Student" },
            new Role { Id = 4, Name = "Guest" }
        };
        var users = new List<User>()
        {
            new User() { Id = 1 }, new User() { Id = 2 }, new User() { Id = 3 }, new User() { Id = 4 }, new User() { Id = 5 }
        };
        var classroomUsers = new List<ClassroomUser>()
        {
            new ClassroomUser()
            {
                Id = 1,
                UserId = 1,
                ClassroomId = 1,
                RoleId = 2,
            },
            new ClassroomUser()
            {
                Id = 2,
                UserId = 2,
                ClassroomId = 2,
                RoleId = 2,
            },
            new ClassroomUser()
            {
                Id = 3,
                UserId = 3,
                ClassroomId = 1,
                RoleId = 3,
            },
            new ClassroomUser()
            {
                Id = 4,
                UserId = 5,
                ClassroomId = 1,
                RoleId = 3,
            }
        };

        // Setup update data
        var updatedClassroomUsers = new List<ClassroomUser>()
        {
            new ClassroomUser() // should be removed due to invalid role id
            {
                Id = 1,
                UserId = 1,
                ClassroomId = 1,
                RoleId = 1,
            },
            new ClassroomUser() // shouldn't be updated due it's owend by another classroom
            {
                Id = 2,
                UserId = 2,
                ClassroomId = 1,
                RoleId = 2,
            },
            new ClassroomUser() // should be updated
            {
                Id = 3,
                UserId = 3,
                ClassroomId = 1,
                RoleId = 4,
            },
            new ClassroomUser() // should be added
            {
                UserId = 5,
                ClassroomId = 1,
                RoleId = 3,
            },
            new ClassroomUser() // shouldn't be added due to already present
            {
                UserId = 3,
                ClassroomId = 1,
                RoleId = 2,
            },
            // classroom user with id equal to 4 should be removed
        };

        _context.Programs.Add(program);
        _context.Courses.Add(course);
        _context.Classrooms.AddRange(classrooms);
        _context.Roles.AddRange(roles);
        _context.Users.AddRange(users);
        _context.ClassroomUsers.AddRange(classroomUsers);

        await _context.SaveChangesAsync();
        _context.ChangeTracker.Clear();

        // Act
        var result = await classroomUserRepository.UpdateClassroomUsersAsync(1, updatedClassroomUsers);

        // Assert
        result.Should().NotBeNull();
        result.Count().Should().Be(2);
        result.Any(u => u.UserId == 1 || u.UserId == 4).Should().Be(false); // user with id 1 and 4 should be removed
        result.Where(u => u.Id == 3).First().RoleId.Should().Be(4); // user with id 3 should be modified
        result.Any(u => u.UserId == 5).Should().Be(true); // user with id 5 should be added
    }
}