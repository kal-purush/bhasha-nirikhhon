
using Microsoft.Extensions.Configuration;
using System.IO;

namespace TestFramework.Config
{
    public class ConfigReader
    {
        public static void SetFrameworkSettings()
        {

            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json");


            IConfigurationRoot configurationRoot = builder.Build();


            Settings.URL = configurationRoot.GetSection("testSettings").Get<TestSettings>().URL;
            Settings.BrowserType = configurationRoot.GetSection("testSettings").Get<TestSettings>().Browser;

        }
    }
}