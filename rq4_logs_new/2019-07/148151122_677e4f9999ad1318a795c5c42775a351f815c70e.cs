using Microsoft.Extensions.DependencyInjection;
using SFA.DAS.Authorization.DependencyResolution;
using SFA.DAS.Authorization.ProviderPermissions.Handlers;
using SFA.DAS.ProviderRelationships.Api.Client.DependencyResolution.Microsoft;

namespace SFA.DAS.Authorization.ProviderPermissions.DependencyResolution
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddProviderPermissionsAuthorization(this IServiceCollection services)
        {
            return services.AddAuthorizationHandler<AuthorizationHandler>()
                .AddProviderRelationshipsApiClient();
        }
    }
}