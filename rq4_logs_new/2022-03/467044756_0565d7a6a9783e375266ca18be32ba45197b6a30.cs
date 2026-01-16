using HexMaster.Email.Abstractions.Services;
using HexMaster.Email.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace HexMaster.Email.Configuration
{
    public static class ServiceCollectionExtensions
    {

        public static void ConfigureEmailService(this IServiceCollection serviceCollection, IConfigurationSection section)
        {
            serviceCollection.Configure<EmailOptions>(section);
            serviceCollection.AddScoped<IMailService, MailService>();
        }
    }
}