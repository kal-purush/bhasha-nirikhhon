namespace ApiaryManagementSystem.Application.IntegrationTests;

using ApiaryManagementSystem.Application.Common.Interfaces;
using MediatR;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

public abstract class BaseIntegrationTest : IClassFixture<IntegrationTestWebAppFactory>, IDisposable
{
    private readonly IServiceScope scope;
    protected readonly ISender sender;
    protected readonly IApplicationDbContext dbContext;

    protected BaseIntegrationTest(IntegrationTestWebAppFactory factory)
    {
        this.scope = factory.Services.CreateScope();

        this.sender = this.scope.ServiceProvider.GetRequiredService<ISender>();
        this.dbContext = this.scope.ServiceProvider.GetRequiredService<IApplicationDbContext>();
    }

    public void Dispose()
    {
        this.scope?.Dispose();

        GC.SuppressFinalize(this);
    }
}