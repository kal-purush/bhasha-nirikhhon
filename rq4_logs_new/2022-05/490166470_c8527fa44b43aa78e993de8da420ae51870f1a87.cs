using InternalSystem.UserManagement.Repository.IRepository;
using InternalSystem.UserManagement.Repository.Repository;
using InternalSystem.UserManagement.Service.IServices;
using InternalSystem.UserManagement.Service.Services;
using InternalSystem.UserManagement.Services.IServices;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Versioning;

namespace InternalSystem.UserManagement.API.Extensions
{
    /// <summary>
    /// Extension methods on IServiceCollection.
    /// Other required extension methods for IServiceCollection can be created in this class.
    /// </summary>
    public static class ServiceCollectionExtensions
    {
        /// <summary>
        /// Register api versioning here.
        /// </summary>
        /// <param name="services">Service collection</param>
        /// <param name="majorVersion">Major version of api</param>
        internal static void AddApiVersioning(this IServiceCollection services, int majorVersion)
        {
            services.AddApiVersioning(c =>
            {
                c.DefaultApiVersion = new ApiVersion(majorVersion, 0);
                c.AssumeDefaultVersionWhenUnspecified = true;
                c.ReportApiVersions = true;
                c.ApiVersionReader = new UrlSegmentApiVersionReader();
            });
        }

        /// <summary>
        /// Register all external/third party services i.e. Azure here.
        /// </summary>
        /// <param name="services"></param>
        /// <param name="configuration"></param>
        internal static void AddExternalServices(this IServiceCollection services, IConfiguration configuration)
        {
        }


        /// <summary>
        /// Register all internal dependencies i.e helpers, wrappers, etc. here.
        /// </summary>
        /// <param name="services"></param>
        /// <param name="configuration"></param>
        internal static void AddInternalServices(this IServiceCollection services, IConfiguration configuration, IWebHostEnvironment environment)
        {
            services.AddScoped<IAuthenticationService, AuthenticationService>();
            services.AddScoped<IUserService, UserService>();
            services.AddScoped<IUserRepository, UserRepository>();
        }


    }
}