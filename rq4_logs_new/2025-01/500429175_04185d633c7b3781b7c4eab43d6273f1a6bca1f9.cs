using Crypto.Infrastructure.Persistence;
using MassTransit;
using MassTransit.EntityFrameworkCoreIntegration;

namespace Crypto.Infrastructure.Extensions;

public static class MassTransitExtensions
{
    public static void AddQueueConsumer<TConsumer>(this IBusRegistrationConfigurator configurator, bool isTemporary)
        where TConsumer : class, IConsumer
    {
        configurator.AddConsumer<TConsumer>()
            .Endpoint(config =>
            {
                config.Temporary = isTemporary;
            });
    }

    public static void AddStateMachine<TStateMachine, TState>(this IBusRegistrationConfigurator configurator,
        bool isTemporary)
        where TStateMachine : class, SagaStateMachine<TState>
        where TState : class, SagaStateMachineInstance
    {
        configurator.AddSagaStateMachine<TStateMachine, TState>()
            .EntityFrameworkRepository(r =>
            {
                r.ExistingDbContext<CryptoDbContext>();
                r.LockStatementProvider = new PostgresLockStatementProvider();
                r.UsePostgres();
            })
            .Endpoint(config =>
            {
                config.Temporary = isTemporary;
            });
    }
}