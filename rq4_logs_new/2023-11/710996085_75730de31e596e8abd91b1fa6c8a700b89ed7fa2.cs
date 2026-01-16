using SecretCode.Api.Features.User.Handlers;
using SecretCode.Api.Features.User.Queries;
using SecretCode.Tests.Global;
using Xunit;

namespace SecretCode.Tests;

[Collection("Fixture Collection")]
public class GetUsersHandlerTests
{
    private readonly GlobalFixture _globalFixture;

    public GetUsersHandlerTests(GlobalFixture globalFixture) => _globalFixture = globalFixture;

    [Fact]
    public async Task Handle_ShouldReturnAllUsers_WhenCalled()
    {
        //Arrange
        GetUsersQuery request = new();
        GetUsersHandler handler = new(_globalFixture._contextMock.Object, _globalFixture._mapperMock.Object);
        
        //Act
        var result = await handler.Handle(request, default);

        //Assert;
        Assert.Equal<int>(result.Count, _globalFixture._users.Count);
    }
}