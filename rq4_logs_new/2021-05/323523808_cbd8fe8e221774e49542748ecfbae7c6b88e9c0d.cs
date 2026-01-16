using FluentAssertions;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Xunit;
using MyRestaurant.Api.Tests.Swagger.Fixture;

namespace MyRestaurant.Api.Tests.Swagger
{
    public class MyRestaurantDevEnvTests : IClassFixture<MyRestaurantDevEnvTestFixture>
    {
        private readonly HttpClient _client;
        public MyRestaurantDevEnvTests(MyRestaurantDevEnvTestFixture fixture)
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
            response.StatusCode.Should().Be(HttpStatusCode.OK);
        }
    }
}