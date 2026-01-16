using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NWCodeFirstMVC.App.Contracts;
using NWCodeFirstMVC.Infrastructure.Services;

namespace NWCodeFirstMVC.Infrastructure.Extension
{
    public static class ConfigureServices
    {
        public static IServiceCollection ConfigureService(this IServiceCollection services)
        {
            services.AddScoped<IProductService, ProductService>();

            return services;
        }
    }
}