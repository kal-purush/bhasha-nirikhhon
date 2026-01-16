namespace ApiaryManagementSystem.Application.IntegrationTests.Features.Apiaries.Queries;

using ApiaryManagementSystem.Application.Features.Apiaries.Queries.GetApiaries;
using ApiaryManagementSystem.Domain.Models.Apiaries;
using AutoFixture;
using FluentAssertions;
using Xunit;

using static Application.Common.Constants.ApplicationConstants.Pagination;

public class GetApiariesTests(IntegrationTestWebAppFactory factory) : BaseIntegrationTest(factory)
{
    [Fact]
    public async Task GetApiaries_ShouldGetDefaultPageSizeApiariesCount_WhenPageSizeIsNotProvided()
    {
        // Arrange
        var apiariesDbModels = this.fixture
            .Build<Apiary>()
            .Without(x => x.Hives)
            .CreateMany(15);

        this.dbContext.Apiaries.AddRange(apiariesDbModels);
        await this.dbContext.SaveChangesAsync(CancellationToken.None);

        var query = new GetApiariesQuery();

        // Act
        var apiaries = await this.sender.Send(query);

        // Assert
        apiaries.Page.Should().Be(DefaultPage);
        apiaries.PageSize.Should().Be(DefaultPageSize);
        apiaries.Items.Count.Should().Be(DefaultPageSize);
        apiaries.TotalCount.Should().Be(this.dbContext.Apiaries.Count());
    }

    [Fact]
    public async Task GetApiaries_ShouldGetAsManyApiariesAsCan_WhenPageSizeIsMoreThanApiariesCount()
    {
        // Arrange
        var apiariesDbModels = this.fixture
            .Build<Apiary>()
            .Without(x => x.Hives)
            .CreateMany(5);

        this.dbContext.Apiaries.AddRange(apiariesDbModels);
        await this.dbContext.SaveChangesAsync(CancellationToken.None);

        var query = new GetApiariesQuery()
        {
            Page = 1,
            PageSize = 10,
        };

        // Act
        var apiaries = await this.sender.Send(query);

        // Assert
        var allDbContextApiariesCount = this.dbContext.Apiaries.Count();

        apiaries.Page.Should().Be(query.Page);
        apiaries.PageSize.Should().Be(query.PageSize);
        apiaries.Items.Count.Should().Be(allDbContextApiariesCount);
        apiaries.TotalCount.Should().Be(allDbContextApiariesCount);
    }

    [Fact]
    public async Task GetApiaries_ShouldGetAsManyApiariesAsPageSize_WhenPageSizeIsLessThanApiariesCount()
    {
        // Arrange
        var apiariesDbModels = this.fixture
            .Build<Apiary>()
            .Without(x => x.Hives)
            .CreateMany(5);

        this.dbContext.Apiaries.AddRange(apiariesDbModels);
        await this.dbContext.SaveChangesAsync(CancellationToken.None);

        var query = new GetApiariesQuery()
        {
            Page = 1,
            PageSize = 2,
        };

        // Act
        var apiaries = await this.sender.Send(query);

        // Assert
        apiaries.Page.Should().Be(query.Page);
        apiaries.PageSize.Should().Be(query.PageSize);
        apiaries.Items.Count.Should().Be(query.PageSize);
        apiaries.TotalCount.Should().Be(this.dbContext.Apiaries.Count());
    }

    [Fact]
    public async Task GetApiaries_ShouldGetRemainingApiariesFromLastPage_WhenPageIsLastOne()
    {
        // Arrange
        var apiariesDbModels = this.fixture
            .Build<Apiary>()
            .Without(x => x.Hives)
            .CreateMany(8);

        this.dbContext.Apiaries.AddRange(apiariesDbModels);
        await this.dbContext.SaveChangesAsync(CancellationToken.None);

        var query = new GetApiariesQuery()
        {
            Page = 2,
            PageSize = 5,
        };

        // Act
        var apiaries = await this.sender.Send(query);

        // Assert
        var allDbContextApiariesCount = this.dbContext.Apiaries.Count();
        var reminder = allDbContextApiariesCount % query.PageSize;

        apiaries.Page.Should().Be(query.Page);
        apiaries.PageSize.Should().Be(query.PageSize);
        apiaries.Items.Count.Should().Be(reminder);
        apiaries.TotalCount.Should().Be(allDbContextApiariesCount);
    }

    [Fact]
    public async Task GetApiaries_ShouldGetOnlySearchedApiaries_WhenSearchTermIsProvided()
    {
        // Arrange
        var firstApiary = new Apiary("Test", "Valid");
        var secondApiary = new Apiary("Valid", "Test");

        var thirdApiary = new Apiary("Invalid Name", "Invalid Location");

        this.dbContext.Apiaries.Add(firstApiary);
        this.dbContext.Apiaries.Add(secondApiary);
        this.dbContext.Apiaries.Add(thirdApiary);
        await this.dbContext.SaveChangesAsync(CancellationToken.None);

        var query = new GetApiariesQuery()
        {
            SearchTerm = "Test"
        };

        // Act
        var apiaries = await this.sender.Send(query);

        // Assert
        apiaries.Items.Count.Should().Be(2);
        apiaries.TotalCount.Should().Be(2);
    }
}