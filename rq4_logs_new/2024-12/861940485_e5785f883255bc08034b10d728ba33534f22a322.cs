namespace ApiaryManagementSystem.Application.IntegrationTests.Features.Apiaries;

using ApiaryManagementSystem.Application.Features.Apiaries.Commands.UpdateApiary;
using ApiaryManagementSystem.Domain.Models.Apiaries;
using Ardalis.GuardClauses;
using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using Xunit;

public class UpdateApiaryTests(IntegrationTestWebAppFactory factory) : BaseIntegrationTest(factory)
{
    [Fact]
    public async Task Update_ShouldUpdateApiary_WhenApiaryExists()
    {
        // Arrange
        var apiary = new Apiary("Test Apiary", "Test Location");
        this.dbContext.Apiaries.Add(apiary);
        await this.dbContext.SaveChangesAsync(CancellationToken.None);

        var command = new UpdateApiaryCommand()
        {
            Id = apiary.Id,
            Name = "Updated Apiary Name",
            Location = "Updated Apiary Location",
        };

        // Act
        await this.sender.Send(command);

        // Assert
        var updatedApiary = await this.dbContext.Apiaries.FirstOrDefaultAsync(x => x.Id == apiary.Id);

        updatedApiary.Should().NotBeNull();
        updatedApiary!.Name.Should().Be(command.Name);
        updatedApiary.Location.Should().Be(command.Location);
    }

    [Fact]
    public async Task Update_ShouldThrowNotFoundException_WhenApiaryDoesNotExists()
    {
        //Arrange
        var command = new UpdateApiaryCommand()
        {
            Id = Guid.NewGuid(),
            Name = "Test Name",
            Location = "Test Location"
        };

        // Act
        var act = async () => await this.sender.Send(command);

        // Assert
        await act.Should().ThrowAsync<NotFoundException>();
    }
}