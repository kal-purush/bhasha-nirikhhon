using Domain.Data;

using Domain.Repository;
using Domain.Repository.IRepository;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Domain
{
    public static class DependencyInjection
    {
        public static IServiceCollection AddDomain(this IServiceCollection services, IConfiguration configuration)
        {

            services.AddDbContext<MyDbContext>(options =>
            options.UseMySQL(
                configuration.
             
                GetConnectionString("MyDbContext")
                ?? throw new InvalidOperationException("Connection string 'MyDbContext' not found.")
                ));


            services.AddScoped<MyDbContext>();

            return services;
        }

        public static IServiceCollection AddServices(this IServiceCollection services, IConfiguration configuration)
        {

          //  services.AddScoped<IGenericRepository<Product>, GenericRepository<Product>>();
            services.AddScoped<IProductRepository, ProductRepository>();



            return services;
        }
    }
}