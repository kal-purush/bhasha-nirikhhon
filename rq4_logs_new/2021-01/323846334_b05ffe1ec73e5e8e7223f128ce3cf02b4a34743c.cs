using CDListingTests.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;
using Xunit.DependencyInjection;
using Xunit.DependencyInjection.Logging;

namespace CDListingTests
{
    public interface IDependency
    {
        ITestOutputHelper OutputHelper { get; }
    }
    
    internal class DependencyClass : IDependency
    {
        private readonly ITestOutputHelperAccessor _testOutputHelperAccessor;

        public DependencyClass(ITestOutputHelperAccessor testOutputHelperAccessor)
        {
            _testOutputHelperAccessor = testOutputHelperAccessor;
        }
        public ITestOutputHelper OutputHelper => _testOutputHelperAccessor.Output;
    }

    public class Startup
    {
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddTransient<IDependency, DependencyClass>();
        }

        public void Configure(ILoggerFactory loggerFactory, ITestOutputHelperAccessor accessor) =>
            loggerFactory.AddProvider(new XunitTestOutputLoggerProvider(accessor));

        public Startup()
        {
            var configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json")
                .AddEnvironmentVariables()
                .Build();

            configuration.Bind(ConfigManager.GetInstance());
        }
    }
}