using Microsoft.Extensions.Configuration;
using Microsoft.AspNetCore.Hosting;

namespace TestApp.Core.Common.Extensions
{
    public static class ConfigurationBuilderExtensions
    {
        public static IWebHostBuilder UseCliUrls(this IWebHostBuilder builder, string[] args)
        {
            var config = new ConfigurationBuilder()
                .AddCommandLine(args)
                .Build();

            var hostUrl = config["hosturl"];

            if (string.IsNullOrWhiteSpace(hostUrl))
            {
                return builder;
            }
            else
            {
                return builder.UseUrls(hostUrl);
            }
        }
    }
}