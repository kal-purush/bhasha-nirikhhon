using DevIO.Business.Interfaces.Notifications;
using DevIO.Business.Interfaces.Repository;
using DevIO.Business.Services;
using DevIO.Utils.Tests.Builders.Business.Models;
using FluentAssertions;
using Moq;
using Moq.AutoMock;

namespace DevIO.Business.Tests.Services;

public class EnderecoServiceTests
{
    private const string ClassName = nameof(EnderecoService);

    private readonly Mock<IEnderecoRepository> _enderecoRepository;
    private readonly Mock<INotifier> _notifier;
    private readonly EnderecoService _enderecoService;

    public EnderecoServiceTests()
    {
        var mocker = new AutoMocker();
        _enderecoService = mocker.CreateInstance<EnderecoService>();
        _enderecoRepository = mocker.GetMock<IEnderecoRepository>();
        _notifier = mocker.GetMock<INotifier>();
    }

    [Fact]  
    public async Task GetAllAsync_ShouldReturnAllAddresses()
    {
        // Arrange
        var enderecos = EnderecoBuilder.Instance
            .BuildCollection()
            .ToList();

        _enderecoRepository
            .Setup(service => service.GetAllAsync())
            .ReturnsAsync(enderecos);

        // Act
        var result = await _enderecoService.GetAllAsync();

        // Assert
        result.Should().NotBeNull();
        result.Count().Should().Be(enderecos.Count);

        _enderecoRepository.Verify(
            service => service.GetAllAsync(), 
            Times.Once());
    }
}