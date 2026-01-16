using FluentAssertions;
using MyRestaurant.Api.Tests.Swagger.Fixture;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Xunit;

namespace MyRestaurant.Api.Tests.Swagger
{
    public class MyRestaurantProdEnvTests : IClassFixture<MyRestaurantProdEnvTestFixture>
    {
        private readonly HttpClient _client;
        public MyRestaurantProdEnvTests(MyRestaurantProdEnvTestFixture fixture)
        {
            // Arrange
            _client = fixture.CreateClient();
        }

        [Fact]
        public async Task Development_Environment_Can_Access_Swagger()
        {
            // Act
            var response = await _client.GetAsync("/swagger/index.html");

            // Assert
            response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
        }
    }
}