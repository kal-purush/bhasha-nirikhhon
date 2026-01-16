using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Http;
using Net.Http.Utils.Logging;

namespace Microsoft.Extensions.DependencyInjection
{
    public static class HttpClientLoggingDependencyInjectionExtensions
    {
        public static IServiceCollection AddHttpClientLogging(this IServiceCollection services)
        {
            services.TryAddEnumerable(ServiceDescriptor.Singleton<IHttpMessageHandlerBuilderFilter, 
                HttpClientMessageHandlerBuilderFilter>());
            return services;
        }
    }
}