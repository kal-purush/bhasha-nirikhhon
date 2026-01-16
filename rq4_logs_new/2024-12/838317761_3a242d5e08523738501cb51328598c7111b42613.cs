using Testcontainers.PostgreSql;

namespace Wordflux.Tests.Integration.Containers;

public class PostgresContainerFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder()
        .WithImage("postgres:latest")
        .WithName($"as_postgres_{Guid.NewGuid()}")
        .WithCleanUp(true)
        .Build();

    public string ConnectionString => _container.GetConnectionString();

    public virtual Task InitializeAsync()
        => _container.StartAsync();

    public virtual Task DisposeAsync()
        => _container.DisposeAsync().AsTask();
}