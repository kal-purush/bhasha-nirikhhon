using Xunit;

using Microsoft.AspNetCore.Mvc.Testing;

using System.Net;
using System.Net.Http;
using System.Net.Http.Json;
using System.Threading.Tasks;

using Model;

namespace Minimal.API.Rest.Tests;

public class IntegrationTests : IClassFixture<WebApplicationFactory<Program>>
{
    private readonly HttpClient _httpClient;

    public IntegrationTests(WebApplicationFactory<Program> factory)
    {
        _httpClient = factory.CreateClient();
    }

    [Fact]
    public async Task EndToEndSpec()
    {
        // /reset - Reset state before starting tests
        {
            string endpoint = "/reset";
            HttpResponseMessage response = await _httpClient.PostAsJsonAsync(endpoint, "");
            string content = await response.Content.ReadAsStringAsync();

            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal("OK", content);
        }

        // /balance - Get balance for non-existing account
        {
            string endpoint = "/balance?account_id=1234";
            HttpResponseMessage response = await _httpClient.GetAsync(endpoint);
            string content = await response.Content.ReadAsStringAsync();

            Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
            Assert.Equal("0", content);
        }

        // /event - Create account with initial balance
        {
            string endpoint = "/event";
            Event payload = new()
            {
                Type = EventType.Deposit,
                Destination = "100",
                Amount = 10
            };
            Transaction expected = new()
            {
                Destination = new()
                {
                    Id = "100",
                    Balance = 10
                }
            };
            HttpResponseMessage response = await _httpClient.PostAsJsonAsync(endpoint, payload);
            Transaction? result = await response.Content.ReadFromJsonAsync<Transaction>();

            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            Assert.NotNull(result);
            Assert.NotNull(result!.Destination);
            Assert.Equal(expected.Destination.Id, result!.Destination!.Id);
            Assert.Equal(expected.Destination.Balance, result!.Destination!.Balance);
        }

        // /event - Deposit into existing account
        {
            string endpoint = "/event";
            Event payload = new()
            {
                Type = EventType.Deposit,
                Destination = "100",
                Amount = 10
            };
            Transaction expected = new()
            {
                Destination = new()
                {
                    Id = "100",
                    Balance = 20
                }
            };
            HttpResponseMessage response = await _httpClient.PostAsJsonAsync(endpoint, payload);
            Transaction? result = await response.Content.ReadFromJsonAsync<Transaction>();

            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            Assert.NotNull(result);
            Assert.NotNull(result!.Destination);
            Assert.Equal(expected.Destination.Id, result!.Destination!.Id);
            Assert.Equal(expected.Destination.Balance, result!.Destination!.Balance);
        }

        // /balance - Get balance for existing account
        {
            string endpoint = "/balance?account_id=100";
            HttpResponseMessage response = await _httpClient.GetAsync(endpoint);
            string content = await response.Content.ReadAsStringAsync();

            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal("20", content);
        }

        // /event - Withdraw from non-existing account
        {
            string endpoint = "/event";
            Event payload = new()
            {
                Type = EventType.Withdraw,
                Origin = "200",
                Amount = 10
            };
            HttpResponseMessage response = await _httpClient.PostAsJsonAsync(endpoint, payload);
            string content = await response.Content.ReadAsStringAsync();

            Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
            Assert.Equal("0", content);
        }

        // /event - Withdraw from existing account
        {
            string endpoint = "/event";
            Event payload = new()
            {
                Type = EventType.Withdraw,
                Origin = "100",
                Amount = 5
            };
            Transaction expected = new()
            {
                Origin = new()
                {
                    Id = "100",
                    Balance = 15
                }
            };
            HttpResponseMessage response = await _httpClient.PostAsJsonAsync(endpoint, payload);
            Transaction? result = await response.Content.ReadFromJsonAsync<Transaction>();
            
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            Assert.NotNull(result);
            Assert.NotNull(result!.Origin);
            Assert.Equal(expected.Origin.Id, result!.Origin!.Id);
            Assert.Equal(expected.Origin.Balance, result!.Origin!.Balance);
        }

        // /event - Transfer from existing account
        {
            string endpoint = "/event";
            Event payload = new()
            {
                Type = "transfer",
                Origin = "100",
                Destination = "300",
                Amount = 15
            };
            Transaction expected = new()
            {
                Origin = new()
                {
                    Id = "100",
                    Balance = 0
                },
                Destination = new()
                {
                    Id = "300",
                    Balance = 15
                }
            };
            HttpResponseMessage response = await _httpClient.PostAsJsonAsync(endpoint, payload);
            Transaction? result = await response.Content.ReadFromJsonAsync<Transaction>();

            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            Assert.NotNull(result);
            Assert.NotNull(result!.Origin);
            Assert.NotNull(result!.Destination);
            Assert.Equal(expected.Origin.Id, result!.Origin!.Id);
            Assert.Equal(expected.Origin.Balance, result!.Origin!.Balance);
            Assert.Equal(expected.Destination.Id, result!.Destination!.Id);
            Assert.Equal(expected.Destination.Balance, result!.Destination!.Balance);
        }

        // /event - Transfer from non-existing account
        {
            string endpoint = "/event";
            Event payload = new()
            {
                Type = "transfer",
                Origin = "200",
                Destination = "300",
                Amount = 15
            };
            HttpResponseMessage response = await _httpClient.PostAsJsonAsync(endpoint, payload);
            string content = await response.Content.ReadAsStringAsync();

            Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
            Assert.Equal("0", content);
        }
    }

}