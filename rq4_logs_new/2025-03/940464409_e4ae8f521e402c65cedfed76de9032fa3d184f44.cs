namespace MessageBroker.Server.Extensions;

public static class CorsExtensions
{
    public static IServiceCollection AddCustomCors(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddCors(options =>
        {
            options.AddPolicy("AllowSpecificOrigin", builder =>
            {
                builder.WithOrigins(configuration["Cors:AllowedOrigins"]?.Split(",") ?? new string[] { })
                    .AllowAnyMethod()
                    .AllowAnyHeader();
            });
        });

        return services;
    }
}