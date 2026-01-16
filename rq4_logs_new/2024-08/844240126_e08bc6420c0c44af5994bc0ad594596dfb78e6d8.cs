using BoreholeData;
using BoreholeData.Entities;
using BoreholeData.Repositories;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging;

namespace BoreholeUnitTest
{
    [TestClass]
    public class BoreholeRepositoryTests
    {
        private BoreholeRepository repository;

        [TestInitialize]
        public void Init()
        {
            DbContextOptionsBuilder<BoreholeContext> builder = new DbContextOptionsBuilder<BoreholeContext>();
            builder.UseInMemoryDatabase("TestDB");

            using ILoggerFactory factory = LoggerFactory.Create(builder => builder.AddConsole());
            ILogger logger = factory.CreateLogger("Program");

            repository = new BoreholeRepository(new BoreholeData.BoreholeContext(builder.Options), logger);
        }

        [TestMethod]
        public void TestAddBorehole()
        {
            repository.Add(new Borehole() { X = 1, Y = 2, Depth = 3 });

            var count = repository.GetAll().Count();

            Assert.AreEqual(1, count);
        }
    }
}