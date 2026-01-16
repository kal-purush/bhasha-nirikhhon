using Microsoft.Extensions.Configuration;
using System.Globalization;
using System.Linq;

namespace Losol.Identity.Extensions
{
    public static class ConfigurationExtensions
    {
        public static CultureInfo GetDefaultCulture(this IConfiguration config)
        {
            return new CultureInfo(config["Localization:DefaultLocale"]);
        }

        public static CultureInfo[] GetSupportedCultures(this IConfiguration config)
        {
            return config.GetSection("Localization:SupportedCultures")
                .GetChildren()
                .Select(c => new CultureInfo(c.Value))
                .ToArray();
        }
    }
}