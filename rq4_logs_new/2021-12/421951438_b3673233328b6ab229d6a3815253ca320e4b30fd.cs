using Microsoft.Extensions.Configuration;
using System.IO;
using Talkish.API.Controllers;
using Talkish.Dal.Repositories;
using Xunit;

namespace Talkish.IntegrationTests.Blogs.Queries
{
    public class GetBlogsTests
    {
        private static IConfiguration GetConfig()
        {
            var builder = new ConfigurationBuilder()
              .SetBasePath(Directory.GetCurrentDirectory())
              .AddJsonFile("appsettings.json", true, true)
              .AddEnvironmentVariables();

            return builder.Build();
        }
    }
}