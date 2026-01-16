using System;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using ServicesAbstractions;

namespace Services
{
    public static class ApplicationServicesRegistration
    {

        public static IServiceCollection AddApplicationServices(this IServiceCollection services)
        {

            services.AddScoped<IServiceManager, ServiceManager>();
            services.AddAutoMapper(typeof(AssemblyReference).Assembly);

            return services;
        }
    }
}