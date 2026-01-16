using System;
using Microsoft.Extensions.DependencyInjection;

namespace TondForooshApi.Services;

public static class ServiceRegistrationExtensions
{
    public static void RegisterApiServices(this IServiceCollection services)
    {
        services.AddControllers();
        services.AddCors(options =>
        {
            options.AddPolicy("AllowAll", builder =>
            {
                builder.AllowAnyOrigin()
                       .AllowAnyMethod()
                       .AllowAnyHeader();
            });
        });
    }

    public static void RegisterApiMiddlewares(this IApplicationBuilder app)
    {
        app.UseHttpsRedirection();
        app.UseCors("AllowAll");
        app.UseEndpoints(endpoints =>
        {
            endpoints.MapControllers();
        });
    }
}