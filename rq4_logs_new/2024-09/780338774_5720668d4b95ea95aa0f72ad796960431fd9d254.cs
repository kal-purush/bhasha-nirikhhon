using AutoMapper;
using MB.Application.Features.Artists.Commands.CreateArtist;
using MB.Application.Interfaces.Persistence.Common;
using MB.Domain.Entities;
using Moq;
using Xunit;
using Task = System.Threading.Tasks.Task;

public class CreateArtistCommandHandlerTests
{
    private readonly Mock<IMapper> _mapperMock;
    private readonly Mock<IBaseRepository<Artist>> _artistRepositoryMock;
    private readonly CreateArtistCommandHandler _handler;

    public CreateArtistCommandHandlerTests()
    {
        _mapperMock = new Mock<IMapper>();
        _artistRepositoryMock = new Mock<IBaseRepository<Artist>>();

        _handler = new CreateArtistCommandHandler(_mapperMock.Object, _artistRepositoryMock.Object);
    }

    [Fact]
    [Trait("Features", "Artists")]
    public async Task Handle_ShouldCreateArtistSuccessfully()
    {
        var command = new CreateArtistCommand { Name = "New Artist", Genre = "Pop" };
        var mappedArtist = new Artist { Name = "New Artist", Genre = "Pop", BusinessId = Guid.NewGuid() };

        _mapperMock.Setup(m => m.Map<Artist>(command))
                   .Returns(mappedArtist);

        _artistRepositoryMock.Setup(repo => repo.CreateAsync(mappedArtist))
                             .ReturnsAsync(mappedArtist);

        var cancellationToken = new CancellationToken();

        var result = await _handler.Handle(command, cancellationToken);

        Assert.True(result.Success);
        Assert.Equal($"The artist '{mappedArtist.Name}' has been successfully created.", result.Message);
        Assert.Equal(mappedArtist.BusinessId, result.ArtistId);

        _mapperMock.Verify(m => m.Map<Artist>(command), Times.Once);
        _artistRepositoryMock.Verify(repo => repo.CreateAsync(mappedArtist), Times.Once);
    }

    [Fact]
    [Trait("Features", "Artists")]
    public async Task Handle_ShouldThrowException_WhenRepositoryFails()
    {
        var command = new CreateArtistCommand { Name = "New Artist", Genre = "Pop" };
        var mappedArtist = new Artist { Name = "New Artist", Genre = "Pop", BusinessId = Guid.NewGuid() };

        _mapperMock.Setup(m => m.Map<Artist>(command))
                   .Returns(mappedArtist);

        _artistRepositoryMock.Setup(repo => repo.CreateAsync(mappedArtist))
                             .ThrowsAsync(new Exception("Repository failure"));

        var cancellationToken = new CancellationToken();

        await Assert.ThrowsAsync<Exception>(() => _handler.Handle(command, cancellationToken));

        _mapperMock.Verify(m => m.Map<Artist>(command), Times.Once);
        _artistRepositoryMock.Verify(repo => repo.CreateAsync(mappedArtist), Times.Once);
    }
}