using System.Text.Json;
using System.Net.Http;
using System.Threading.Tasks;
using System.Net;
using Application.Models.Dtos;
using Tests.TestExtensions;

namespace Tests.Tests;

public class TestIntegrationTests : TestsBase
{
    public TestIntegrationTests(TestingFactory<Program> factory) : base(factory)
    {
    }
    
    [Fact]
    public async Task GetTests_AfterSuccessfulLogin_ReturnsTestsList()
    {
        await LoginAsync("User1");
        
        var userResponse = await _httpClient.GetAsync("users/get");
        userResponse.EnsureSuccessStatusCode();
        
        var userStream = await userResponse.Content.ReadAsStreamAsync();
        var options = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
        };
        var userDto = await JsonSerializer.DeserializeAsync<UserDto>(userStream, options);

        Assert.NotNull(userDto);
        
        var response = await _httpClient.GetAsync($"tests/list/{userDto.Id}");
        response.EnsureSuccessStatusCode();

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }
}