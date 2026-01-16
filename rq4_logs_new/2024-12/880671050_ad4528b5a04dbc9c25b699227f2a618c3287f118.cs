using System.Net;
using System.Net.Mime;
using System.Text;
using System.Text.Json;
using Bogus;
using FluentAssertions;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SIT.API.Models;
using SIT.Core.Application.Customers.Commands.Requests;
using SIT.Core.Application.Customers.Commands.Results;
using SIT.Core.Domain.Entities;
using SIT.Core.Domain.Enums;
using SIT.Infrastructure.Contexts;
using SIT.Shared.Extensions;
using Xunit.Categories;

namespace SIT.IntegrationTest.Controllers;

[IntegrationTest]
public class CustomerControllerTest : IAsyncLifetime
{
    private const string ConnectionString = "Data Source=:memory:";
    private const string Endpoint = "api/customer";
    private readonly SqliteConnection _writeDbContextSqlite = new SqliteConnection(ConnectionString);

    [Fact]
    public async Task When_Posting_ValidCustomer_ShoulReturn_200()
    {
        await using var webApplicationFactory = GetWebApplicationFactory();
        using var client = webApplicationFactory.CreateClient(CreateClientOptions());
        
        var customerCommand = new Faker<CreateCustomerCommand>()
            .RuleFor(command => command.Name, f => f.Name.JobArea())
            .RuleFor(command => command.CustomerType, f => f.PickRandom<CustomerType>())
            .Generate();

        var commandJsonString = JsonSerializer.Serialize(customerCommand);
        
        using var content = new StringContent(commandJsonString, Encoding.UTF8, MediaTypeNames.Application.Json);
        using var act = await client.PostAsync(Endpoint, content);
        
        var response = await act.Content.ReadAsStringAsync();
        var deserializedCustomer = response.FromJson<ApiGenericResponse<CreatedCustomerResult>>();
        deserializedCustomer.Should().NotBeNull();
        deserializedCustomer.Success.Should().BeTrue();
        deserializedCustomer.StatusCode.Should().Be(StatusCodes.Status200OK);
        deserializedCustomer.Errors.Should().BeEmpty();
        deserializedCustomer.Result.Should().NotBeNull();
        deserializedCustomer.Result.Id.Should().NotBe(null);
    }

    [Fact]
    public async Task When_Posting_InvalidCustomer_ShoulReturn_400()
    {
        await using var webApplicationFactory = GetWebApplicationFactory();
        using var client = webApplicationFactory.CreateClient(CreateClientOptions());

        var customerCommand = new Faker<CreateCustomerCommand>().Generate();
        var commandJsonString = customerCommand.ToJson();
        
        using var content = new StringContent(commandJsonString, Encoding.UTF8, MediaTypeNames.Application.Json);
        using var act = await client.PostAsync(Endpoint, content);

        act.Should().NotBeNull();
        act.IsSuccessStatusCode.Should().BeFalse();
        act.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        
        var response = await act.Content.ReadAsStringAsync();
        var deserializedCustomer = response.FromJson<ApiGenericResponse<CreatedCustomerResult>>();
        deserializedCustomer.Should().NotBeNull();
        deserializedCustomer.Success.Should().BeFalse();
        deserializedCustomer.StatusCode.Should().Be(StatusCodes.Status400BadRequest);
        deserializedCustomer.Result.Should().BeNull();
        deserializedCustomer.Errors.Should().NotBeNullOrEmpty().And.OnlyHaveUniqueItems();
    }

    public async Task InitializeAsync()
    {
        await _writeDbContextSqlite.OpenAsync();
    }

    public async Task DisposeAsync()
    {
        await _writeDbContextSqlite.DisposeAsync();
    }

    private WebApplicationFactory<Program> GetWebApplicationFactory(
        Action<IServiceCollection> configureServices = null,
        Action<IServiceScope> configureScope = null)
    {
        return new WebApplicationFactory<Program>()
            .WithWebHostBuilder(builder =>
            {
                builder.UseSetting("ConnectionStrings:SqlServerConnection", "InMemory");
                builder.UseEnvironment(Environments.Production);
                builder.ConfigureLogging(logging => logging.ClearProviders());
                builder.ConfigureServices(services =>
                {
                    services.RemoveAll<WriteDbContext>();
                    services.RemoveAll<DbContextOptions<WriteDbContext>>();

                    services.AddDbContext<WriteDbContext>(options => options.UseSqlite(_writeDbContextSqlite));

                    configureServices?.Invoke(services);

                    using var serviceProvider = services.BuildServiceProvider(true);
                    using var serviceScope = serviceProvider.CreateScope();

                    var writeDbContext = serviceScope.ServiceProvider.GetRequiredService<WriteDbContext>();
                    writeDbContext.Database.EnsureCreated();

                    configureScope?.Invoke(serviceScope);

                    writeDbContext.Dispose();
                });
            });
    }

    private static WebApplicationFactoryClientOptions CreateClientOptions()
    {
        return new WebApplicationFactoryClientOptions()
        {
            AllowAutoRedirect = false,
        };
    }
}