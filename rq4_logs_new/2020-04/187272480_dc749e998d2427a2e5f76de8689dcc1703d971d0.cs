using Microsoft.Extensions.DependencyInjection;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RabbitMQ.Client;
using RabbitMQ.Fakes;
using RabbitMQ.Migrations.DependencyInjection;

namespace RabbitMQ.Migrations.Tests
{
    [TestClass]
    public class DependencyInjectionTests
    {
        [TestMethod]
        public void DependencyInjectionTest()
        {
            var rabbitServer = new RabbitServer();

            var serviceCollection = new ServiceCollection();
            serviceCollection.AddSingleton<IConnectionFactory>(new FakeConnectionFactory(rabbitServer));
            serviceCollection.AddRabbitMqMigrations();
            var serviceProvider = serviceCollection.BuildServiceProvider(true);

            var migrator = serviceProvider.GetService<IRabbitMqMigrator>();
            Assert.IsNotNull(migrator);

            var history = serviceProvider.GetService<IRabbitMqHistory>();
            Assert.IsNotNull(history);
        }
    }
}