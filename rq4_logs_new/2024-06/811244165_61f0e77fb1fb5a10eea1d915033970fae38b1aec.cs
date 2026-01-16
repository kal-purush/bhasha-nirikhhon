using HealthMate.API.Mappers;
using HealthMate.BLL.Mappers;

namespace HealthMate.API.DI
{
    public static class DependencyInjection
    {
        public static IServiceCollection AddApiServices(this IServiceCollection services)
        {
            services.AddCors(options =>
            {
                options.AddPolicy("CorsPolicy", builder =>
                {
                    builder.AllowAnyOrigin();
                    builder.AllowAnyHeader();
                    builder.AllowAnyMethod();
                });
            });

            services.AddAutoMapper(typeof(MapperAPIProfile).Assembly, typeof(MapperBllProfile).Assembly);

            return services;
        }
    }
}