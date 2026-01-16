using GiftSuggester.Core.Gifts.Services;
using GiftSuggester.Core.Gifts.Services.Implementations;
using GiftSuggester.Core.Groups.Services;
using GiftSuggester.Core.Groups.Services.Implementations;
using GiftSuggester.Core.Users.Services;
using GiftSuggester.Core.Users.Services.Implementations;
using Microsoft.Extensions.DependencyInjection;

namespace GiftSuggester.Core;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddCore(this IServiceCollection serviceCollection)
    {
        serviceCollection.AddScoped<IGiftService, GiftService>();
        serviceCollection.AddScoped<IGroupService, GroupService>();
        serviceCollection.AddScoped<IUserService, UserService>();

        return serviceCollection;
    }
}