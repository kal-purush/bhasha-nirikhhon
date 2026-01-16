using FluentAssertions;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using MyRestaurant.SeedData.Tests.Fixture;
using Xunit;

namespace MyRestaurant.SeedData.Tests
{
    [Collection("Database")]
    public class WrongMyRestaurantSeedDataTest : IClassFixture<WrongMyRestaurantWebApplicationFactory>
    {
        private readonly WrongMyRestaurantWebApplicationFactory _factory;
        public WrongMyRestaurantSeedDataTest(WrongMyRestaurantWebApplicationFactory factory) => _factory = factory;

        [Fact]
        public async void Initialize_MyRestaurantDatabase_Throws_Exception()
        {
            //Arrange
            var scope = _factory.Services.GetRequiredService<IServiceScopeFactory>().CreateScope();
            var databaseInitializer = scope.ServiceProvider.GetRequiredService<IMyRestaurantSeedData>();
            await databaseInitializer.Initialize();

            //Act
            var exception = await Assert.ThrowsAsync<SqlException>(() => databaseInitializer.Initialize());

            //Assert
            exception.Message.Should().Contain("A network-related or instance-specific error occurred while establishing a connection to SQL Server.");
        }
    }
}