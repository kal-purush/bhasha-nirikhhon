using Microsoft.EntityFrameworkCore;
using Serilog;
using VehiclePassRegister.Data;
using VehiclePassRegister.Profiles;
using VehiclePassRegister.Repositories;
using VehiclePassRegister.Repositories.IRepository;
using VehiclePassRegister.Services;
using VehiclePassRegister.Services.IServices;
namespace VehiclePassRegister.Extensions
{
    public static class ServiceExtensions
    {   //service extension
        public static void ConfigureAppServices(this IServiceCollection services)
        {
            services.AddScoped<IVehicleService, VehicleService>();
            services.AddAutoMapper(typeof(VehicleProfile));
        }

        //dataacess extention
        public static void ConfigureDataAccess(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddScoped<IVehicleRepository, VehicleRepository>();
            services.AddScoped<IUnitOfWork, UnitOfWork>();


            services.AddDbContext<DataContext>(options =>
            options.UseSqlServer(configuration.GetConnectionString("Default")));


        }

        //Serilog configure
        public static void ConfigureLogger(this IHostBuilder host)
        {
            host.UseSerilog((context, config) =>
            {
                config.WriteTo.Console();
                //config.WriteTo.File("H:/logvehicle/log.txt");
            });
        }

    }
}