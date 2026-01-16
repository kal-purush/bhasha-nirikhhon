namespace Blog.Api.Infrastructure;

public static class ConfigCorsExtensions
{
    public static IServiceCollection AddCrossOrigin(this IServiceCollection services)
    {
        services.AddCors(options => {
            options.AddPolicy("DevelopmentCorsPolicy",
                builder => builder
                    .AllowAnyOrigin()
                    .AllowAnyMethod()
                    .AllowAnyHeader()
                );
        });

        return services;
    }


    public static IApplicationBuilder UseCrossOrigin(this WebApplication app)
    {
        if (app.Environment.IsDevelopment())
        {
            app.UseCors("DevelopmentCorsPolicy");
        }

        return app;
    }
}