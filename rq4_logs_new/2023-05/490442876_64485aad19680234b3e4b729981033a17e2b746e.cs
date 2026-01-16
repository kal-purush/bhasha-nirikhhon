using GiftSuggester.Core.CommonClasses;
using GiftSuggester.Core.Gifts.Repositories;
using GiftSuggester.Core.Groups.Repositories;
using GiftSuggester.Core.Users.Repositories;
using GiftSuggester.Data.Gifts.Repositories;
using GiftSuggester.Data.Groups.Repositories;
using GiftSuggester.Data.Users.Repositories;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace GiftSuggester.Data;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddData(this IServiceCollection serviceCollection, IConfiguration configuration)
    {
        serviceCollection.AddScoped<IGiftRepository, GiftRepository>();
        serviceCollection.AddScoped<IGroupRepository, GroupRepository>();
        serviceCollection.AddScoped<IUserRepository, UserRepository>();

        serviceCollection.AddScoped<IUnitOfWork, UnitOfWork>();

        serviceCollection.AddDbContext<GiftSuggesterContext>(options => options
            .UseLazyLoadingProxies()
            .UseNpgsql(configuration["DbConnectionString"]));

        return serviceCollection;
    }
}