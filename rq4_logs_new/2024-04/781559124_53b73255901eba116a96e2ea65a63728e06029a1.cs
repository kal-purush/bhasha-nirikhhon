using Microsoft.Extensions.DependencyInjection;
using RMS.Application.Common.Interfaces;
using RMS.Application.Services;
using RMS.Domain.Entities;
using System.Reflection;


namespace RMS.Application;

 public static class DependencyInjection
{
    public static IServiceCollection AddApplication(this IServiceCollection services)
    {
        services.AddScoped<IBaseService<Customer>, CustomerService>();
        services.AddAutoMapper(Assembly.GetExecutingAssembly());
        return services;
    }
}