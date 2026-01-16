using MealService.Application.Specifications.Repositories;
using MealService.Application.Specifications;
using MealService.Infrastructure.Implementations.Repositories;
using MealService.Infrastructure.Implementations;
using MealService.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;

namespace MealService.GrpcServer
{
    public static class DependencyInjection
    {
        public static void ConfigureServices(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddDbContext<AppDbContext>(option =>
            {
                option.UseSqlServer(configuration.GetConnectionString("DefaultConnection"));
            });

            services.AddScoped<IUnitOfWork, UnitOfWork>();
            services.AddScoped(typeof(IRepository<>), typeof(Repository<>));
        }
    }
}