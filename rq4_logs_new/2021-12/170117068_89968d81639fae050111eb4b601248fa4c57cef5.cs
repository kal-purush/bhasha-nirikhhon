using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using SFA.DAS.Forecasting.Data;
using SFA.DAS.Forecasting.Domain.Configuration;
using System;

namespace SFA.DAS.Forecasting.Api.Extensions
{
    public static class AddDatabaseExtension
    {
        public static void AddDatabaseRegistration(this IServiceCollection services, ForecastingConfiguration config, string environmentName)
        {
            if (environmentName.Equals("LOCAL", StringComparison.CurrentCultureIgnoreCase))
            {
                services.AddDbContext<ForecastingDataContext>(options => options.UseSqlServer(config.ConnectionString), ServiceLifetime.Transient);
            }
            else
            {
                services.AddSingleton(new AzureServiceTokenProvider());
                services.AddDbContext<ForecastingDataContext>(ServiceLifetime.Transient);
            }

            services.AddTransient<IForecastingDataContext, ForecastingDataContext>(provider => provider.GetService<ForecastingDataContext>());
            services.AddTransient(provider => new Lazy<ForecastingDataContext>(provider.GetService<ForecastingDataContext>()));
        }
    }
 }