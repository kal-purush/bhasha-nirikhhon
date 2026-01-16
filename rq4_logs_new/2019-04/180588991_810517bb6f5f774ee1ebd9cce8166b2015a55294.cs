using AutoMapper;
using Catalog.BusinessLogicLayer.Service;
using Catalog.DataAccessLayer;
using Catalog.Domain.Mapping;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;
using XUnit.Helpers;

namespace Catalog.BusinessLogicLayer.UnitTests
{
    public class XRoadOperationalDataProcessorTests
    {
        private readonly ILogger<XRoadOperationalDataProcessor> _logger;

        public XRoadOperationalDataProcessorTests(ITestOutputHelper testOutputHelper)
        {
            var loggerFactory = new LoggerFactory();
            loggerFactory.AddProvider(new XUnitLoggerProvider(testOutputHelper));
            _logger = loggerFactory.CreateLogger<XRoadOperationalDataProcessor>();
        }

        [Fact]
        public void ProcessRecords()
        {
            var dbContextOptionsBuilder = new DbContextOptionsBuilder<CatalogDbContext>()
                .UseNpgsql("Server=localhost;Port=5432;Database=Tunduk;User Id=postgres;Password=postgres;");

            var mapperConfiguration = new MapperConfiguration(cfg =>
            {
                cfg.AddProfiles(typeof(SecurityServerProfile));
                cfg.AddProfiles(typeof(OperationalDataRecordProfile));
            });

            XRoadOperationalDataProcessor xRoadOperationalDataProcessor = new XRoadOperationalDataProcessor(
                _logger,
                dbContextOptionsBuilder.Options,
                mapperConfiguration.CreateMapper()
            );
            xRoadOperationalDataProcessor.ProcessRecords();
        }
    }
}