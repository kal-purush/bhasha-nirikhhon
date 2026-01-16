using UserService.DataAccess.Implementations.Repositories;
using UserService.DataAccess.Implementations;
using UserService.DataAccess.Specifications.Repositories;
using UserService.DataAccess.Specifications;
using Microsoft.EntityFrameworkCore;
using UserService.DataAccess.Data;
using Microsoft.AspNetCore.Identity;
using UserService.DataAccess.Models;

namespace UserService.GrpcServer
{
    public static class DependencyInjection
    {
        public static void ConfigureServices(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddIdentityCore<ApplicationUser>()
                    .AddRoles<IdentityRole<Guid>>()
                    .AddEntityFrameworkStores<ApplicationDbContext>();

            services.AddDbContext<ApplicationDbContext>(option =>
            {
                option.UseSqlServer(
                   configuration.GetConnectionString("DefaultConnection"),
                   sqlServerOptions => sqlServerOptions.EnableRetryOnFailure());
            });

            services.AddScoped<IUnitOfWork, UnitOfWork>();
            services.AddScoped<IUserRepository, UserRepository>();
        }
    }
}