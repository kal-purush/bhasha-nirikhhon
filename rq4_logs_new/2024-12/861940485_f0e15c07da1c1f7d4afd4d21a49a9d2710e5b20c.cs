namespace ApiaryManagementSystem.Application.IntegrationTests.Features.Apiaries.Queries;

using System.Threading.Tasks;
using ApiaryManagementSystem.Application.Features.Apiaries.Queries.GetApiaryById;
using ApiaryManagementSystem.Domain.Models.Apiaries;
using Ardalis.GuardClauses;
using FluentAssertions;
using Xunit;

public class GetApiaryByIdTests(IntegrationTestWebAppFactory factory) : BaseIntegrationTest(factory)
{
    [Fact]
    public async Task GetById_ShouldGetApiary_WhenApiaryExists()
    {
        // Arrange
        var apiary = new Apiary("Test Apiary", "Test Location");
        this.dbContext.Apiaries.Add(apiary);
        await this.dbContext.SaveChangesAsync(CancellationToken.None);

        var query = new GetApiaryByIdQuery()
        {
            ApiaryId = apiary.Id,
        };

        // Act
        var apiaryModel = await this.sender.Send(query);

        //Assert
        apiaryModel.Should().NotBeNull();
        apiaryModel.Name.Should().Be(apiary.Name);
        apiaryModel.Location.Should().Be(apiary.Location);
    }

    [Fact]
    public async Task GetById_ShouldGetApiary_WhenApiaryDoesNotExists()
    {
        // Arrange
        var query = new GetApiaryByIdQuery()
        {
            ApiaryId = Guid.NewGuid(),
        };

        // Act
        var act = async () => await this.sender.Send(query);

        // Assert
        await act.Should().ThrowAsync<NotFoundException>();
    }
}