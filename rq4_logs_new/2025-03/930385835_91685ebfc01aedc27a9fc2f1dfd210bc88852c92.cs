using PetFamily.Accounts.Presentation;
using PetFamily.Species.Presentation.Species;
using PetFamily.Volunteers.Presentation.Volunteers;

namespace PetFamily.Web.ApplicationConfiguration;



public static class ControllersAdder
{
    public static IServiceCollection AddConfiguredControllers(this IServiceCollection services)
    {
        services.AddControllers()
            //.InterceptModelBindingError()
            .AddApplicationPart(typeof(AccountsController).Assembly)
            .AddApplicationPart(typeof(VolunteersController).Assembly)
            .AddApplicationPart(typeof(SpeciesController).Assembly);
        
        return services;
    }
}