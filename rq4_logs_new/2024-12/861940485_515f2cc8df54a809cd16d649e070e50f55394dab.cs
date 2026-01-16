namespace ApiaryManagementSystem.Application.IntegrationTests.Features.Apiaries;

using ApiaryManagementSystem.Application.Features.Apiaries.Commands.DeleteApiary;
using ApiaryManagementSystem.Domain.Models.Apiaries;
using Ardalis.GuardClauses;
using FluentAssertions;
using Xunit;

public class DeleteApiaryTests(IntegrationTestWebAppFactory factory) : BaseIntegrationTest(factory)
{
    [Fact]
    public async Task Delete_ShouldDeleteApiary_WhenApiaryExists()
    {
        // Arrange
        var apiary = new Apiary("Test Apiary", "Test Location");
        this.dbContext.Apiaries.Add(apiary);
        await this.dbContext.SaveChangesAsync(CancellationToken.None);

        var command = new DeleteApiaryCommand()
        {
            Id = apiary.Id,
        };

        // Act
        await this.sender.Send(command);

        // Assert
        var deletedApiary = await this.dbContext.Apiaries.FindAsync(apiary.Id);

        deletedApiary.Should().BeNull();
    }

    [Fact]
    public async Task Delete_ShouldThrowNotFoundException_WhenApiaryDoesNotExists()
    {
        // Arrange
        var command = new DeleteApiaryCommand()
        {
            Id = Guid.NewGuid(),
        };

        // Act
        var act = async () => await this.sender.Send(command);

        // Assert
        await act.Should().ThrowAsync<NotFoundException>();
    }
}