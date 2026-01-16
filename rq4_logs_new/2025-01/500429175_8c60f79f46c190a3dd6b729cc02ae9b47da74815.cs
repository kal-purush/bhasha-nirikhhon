using Crypto.Application.Interfaces.Price;
using Crypto.Infrastructure.Persistence;
using Crypto.Infrastructure.Price;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Respawn;
using Tests.Common.Extensions;
using Tests.Common.Interfaces.Containers;
using Tests.Common.Services.Containers;
using WireMock.Server;

namespace Crypto.IntegrationTests
{
    public class CryptoApiTestContainersFactory : WebApplicationFactory<Program>, IAsyncLifetime
    {
        private readonly IContainerFixture _redisContainer = new RedisFixture();
        private readonly IContainerFixture _mqContainer = new RabbitMqFixture();
        private readonly IContainerFixture _sqlFixture = new TimescaleDBFixture();

        private NpgsqlConnection _dbConnection = default!;
        private Respawner _respawner = default!;
    
        public HttpClient Client { get; private set; } = default!;
        public WireMockServer MockServer { get; private set; } = default!;
    
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            var configuration = new Dictionary<string, string>
            {
                { "CryptoPriceApiOptions:BaseUrl", MockServer.Urls[0] },
                { "CryptoInfoApiOptions:BaseUrl", MockServer.Urls[0] },
                { "ConnectionStrings:CryptoDatabase", _sqlFixture.GetConnectionString() },
                { "QueueOptions:Address", _mqContainer.GetConnectionString() },
                { "RedisOptions:ConnectionString", _redisContainer.GetConnectionString() },
                { "RedisOptions:InstanceName", $"cryptoredistest" },
            }.AsConfiguration();

            builder.UseConfiguration(configuration);
            builder.ConfigureTestServices(services =>
            {
                services.AddScoped<ICryptoPriceService, MockPriceClient>();
                services.AddScoped<DataFixture>();
                services.AddDefaultFakeAuth();
            });
        }

        public async Task InitializeAsync()
        {
            await Task.WhenAll(
                _sqlFixture.StartAsync(),
                _redisContainer.StartAsync(),
                _mqContainer.StartAsync());

            MockServer = WireMockServer.Start();
            Client = CreateClient();

            await using var scope = Services.CreateAsyncScope();
            var dbContext = scope.ServiceProvider.GetRequiredService<CryptoDbContext>();
            await dbContext.Database.MigrateAsync();
            await dbContext.DisposeAsync();

            _dbConnection = new NpgsqlConnection(_sqlFixture.GetConnectionString());
            await _dbConnection.OpenAsync();
            _respawner = await Respawner.CreateAsync(_dbConnection, new RespawnerOptions()
            {
                DbAdapter = DbAdapter.Postgres,
            });
        }
        
        public async Task ResetDatabaseAsync()
        {
            await _respawner.ResetAsync(_dbConnection);
        }

        public new async Task DisposeAsync()
        {
            await Task.WhenAll(
                _sqlFixture.StopAsync(),
                _redisContainer.StopAsync(), 
                _mqContainer.StopAsync());
            
            MockServer.Stop();

            await _dbConnection.DisposeAsync();
        }    
    }
}