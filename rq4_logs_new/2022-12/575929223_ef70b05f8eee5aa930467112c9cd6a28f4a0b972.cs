
using AutoMapper;
using CommandApi.Controllers;
using CommandApi.Data;
using CommandApi.Dtos;
using CommandApi.Models;
using CommandApi.Profiles;
using Microsoft.AspNetCore.Mvc;
using Moq;

namespace CommandAPI.Tests;

public class CommandsControllerTests : IDisposable
{
    private bool disposed = false;
    private readonly Mock<ICommandApiRepo> mockRepo;
    private readonly CommandsProfile realProfile;
    private readonly MapperConfiguration configuration;
    private readonly IMapper mapper;
    public CommandsControllerTests()
    {
        mockRepo = new Mock<ICommandApiRepo>();
        realProfile = new CommandsProfile();
        configuration = new MapperConfiguration(cfg => cfg.
        AddProfile(realProfile));
        mapper = new Mapper(configuration);
    }

    public void Dispose()
    {
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!disposed)
        {
            // mockRepo = null;
            // mapper = null;
            // configuration = null;
            // realProfile = null;

            disposed = true;
        }
    }

    [Fact]
    public void GetCommandItems_ReturnsZeroItems_WhenDBIsEmpty()
    {
        //Arrange
        mockRepo.Setup(repo => repo.GetAllCommands()).Returns(GetCommands(0));

        var controller = new CommandController(mockRepo.Object, mapper);

        //Act
        var result = controller.GetAllCommands();

        //Assert
        Assert.IsType<OkObjectResult>(result.Result);
    }

    [Fact]
    public void GetAllCommands_ReturnsOneItem_WhenDBHasOneResource()
    {
        //Arrange
        mockRepo.Setup(repo => repo.GetAllCommands()).Returns(GetCommands(1));

        var controller = new CommandController(mockRepo.Object, mapper);
        //Act
        ActionResult<IEnumerable<CommandReadDto>> result = controller.GetAllCommands();
        //Assert
        var okResult = result.Result as OkObjectResult;
        var commands = okResult?.Value as List<CommandReadDto>;
        commands ??= new List<CommandReadDto>();
        Assert.Single(commands);
    }

    [Fact]
    public void GetAllCommands_Returns200OK_WhenDBHasOneResource()
    {
        //Arrange
        mockRepo.Setup(repo =>
        repo.GetAllCommands()).Returns(GetCommands(1));
        var controller = new CommandController(mockRepo.Object, mapper);
        //Act
        var result = controller.GetAllCommands();
        //Assert
        Assert.IsType<OkObjectResult>(result.Result);
    }

    [Fact]
    public void GetAllCommands_ReturnsCorrectType_WhenDBHasOneResource()
    {
        //Arrange
        mockRepo.Setup(repo =>
        repo.GetAllCommands()).Returns(GetCommands(1));
        var controller = new CommandController(mockRepo.Object, mapper);
        //Act
        var result = controller.GetAllCommands();
        //Assert
        Assert.IsType<ActionResult<IEnumerable<CommandReadDto>>>(result);
    }

    private static List<Command> GetCommands(int num)
    {
        var commands = new List<Command>();
        if (num > 0)
        {
            commands.Add(new Command
            {
                Id = 0,
                HowTo = "How to generate a migration",
                CommandLine = "dotnet ef migrations add <Name of Migration>",
                Platform = ".Net Core EF"
            });
        }
        return commands;
    }
}
