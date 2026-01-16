using System.Reflection;
using PetFamily.Application.Inject;
using PetFamily.Infrastructure.Inject;
using Serilog;

namespace PetFamily.API.Common;

public static class ServicesInstaller
{
    public static IServiceCollection AddServices(this IServiceCollection services, IConfiguration configuration)
    {
        AddLogger(configuration);

        services.AddEndpointsApiExplorer();
        services.AddSwaggerGen(options =>
        {
            var xmlFilename = $"{Assembly.GetExecutingAssembly().GetName().Name}.xml";
            options.IncludeXmlComments(Path.Combine(AppContext.BaseDirectory, xmlFilename));
        });

        services.AddControllers();

        services.AddHttpLogging(o => { });
        services.AddApplication();
        services.AddInfrastructure();

        services.AddSerilog();

        return services;
    }

    private static void AddLogger(IConfiguration configuration)
    {
        Log.Logger = new LoggerConfiguration()
            .WriteTo.Console()
            .WriteTo.Debug()
            .WriteTo.Seq(configuration.GetConnectionString("Seq") ?? throw new ArgumentNullException("Seq"))
            .CreateLogger();
    }
}