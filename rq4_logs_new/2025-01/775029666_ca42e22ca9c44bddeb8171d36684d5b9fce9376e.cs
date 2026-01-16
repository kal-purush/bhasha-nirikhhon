using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Backend.Fx.Execution;
using Backend.Fx.Execution.Pipeline;
using Backend.Fx.Logging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Backend.Fx.DataSeeding.Feature;

public class DefaultDataSeedingContext : DataSeedingContext
{
    private readonly ILogger _logger = Log.Create<DefaultDataSeedingContext>();

    public DefaultDataSeedingContext(DataSeedingLevel level) : base(level)
    {
    }

    protected override async Task RunSeederInSeparateInvocationAsync(
        IBackendFxApplication application,
        Type seederType,
        CancellationToken cancellationToken)
    {
        await application.Invoker.InvokeAsync(
            async (sp, ct) =>
            {
                var dataSeeders = sp.GetServices<IDataSeeder>().ToArray();
                var dataSeeder = dataSeeders.First(s => s.GetType() == seederType);
                if (dataSeeder.Level >= Level)
                {
                    await dataSeeder.SeedAsync(ct);
                }
                else
                {
                    _logger.LogInformation(
                        "Skipping {SeederLevel} seeder {SeederType} because it is not active for level {Level}",
                        dataSeeder.Level,
                        seederType.Name,
                        Level);
                }
            },
            new SystemIdentity(),
            cancellationToken);
    }
}