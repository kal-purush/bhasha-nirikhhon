using AnimalShelter.Application.Services;
using Microsoft.Extensions.DependencyInjection;

namespace AnimalShelter.Application.Extensions
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddApplication(this IServiceCollection services)
        {
            services.AddScoped<IAdoptionApplicationsService, AdoptionApplicationsService>();
            services.AddScoped<IAdoptionsService, AdoptionsService>();
            services.AddScoped<IAnimalsService, AnimalsService>();
            services.AddScoped<IAnimalStatusesService, AnimalStatusesService>();
            services.AddScoped<IEmployeesService, EmployeesService>();
            services.AddScoped<IRolesService, RolesService>();
            services.AddScoped<IStatusAdoptionsService, StatusAdoptionsService>();
            services.AddScoped<ITemporaryAccommodationsService, TemporaryAccommodationsService>();
            services.AddScoped<ITypeAnimalsService, TypeAnimalsService>();
            services.AddScoped<IUsersService, UsersService>();

            return services;
        }
    }
}