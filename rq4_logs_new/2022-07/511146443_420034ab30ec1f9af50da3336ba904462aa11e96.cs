using Microsoft.AspNetCore.Builder;

namespace QuokkaDev.Saas.Authentication
{
    public static class IApplicationBuilderExtensions
    {
        /// <summary>
        /// Use the Teanant Auth to process the authentication handlers
        /// </summary>
        /// <param name="builder"></param>
        /// <returns></returns>
        public static IApplicationBuilder UseMultiTenantAuthentication(this IApplicationBuilder builder)
            => builder.UseMiddleware<TenantAuthMiddleware>();
    }
}