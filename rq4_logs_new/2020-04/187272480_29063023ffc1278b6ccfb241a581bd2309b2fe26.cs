using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Fakes;
using RabbitMQ.Migrations.Objects.v1;
using System.Linq;
using System.Text;

namespace RabbitMQ.Migrations.Tests
{
    [TestClass]
    public class RabbitMqHistoryV1Tests
    {
        private RabbitServer _rabbitServer;
        private IConnectionFactory _connectionFactory;
        private IRabbitMqHistory _rabbitMqHistory;

        [TestInitialize]
        public void TestInitialize()
        {
            _rabbitServer = new RabbitServer();
            _connectionFactory = new FakeConnectionFactory(_rabbitServer);
            _rabbitMqHistory = new RabbitMqHistory(_connectionFactory);
        }

        [TestMethod]
        public void TestGetAllAppliedMigrationsNoQueue()
        {
            var result = _rabbitMqHistory.GetAllAppliedMigrations();

            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Version);
            Assert.AreEqual(0, result.AllMigrations.Count);
        }

        [TestMethod]
        public void TestGetAllAppliedMigrationsEmptyQueue()
        {
            _rabbitMqHistory.Init();
            var result = _rabbitMqHistory.GetAllAppliedMigrations();

            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Version);
            Assert.AreEqual(0, result.AllMigrations.Count);
        }

        [TestMethod]
        public void TestGetAllAppliedMigrationsFilledQueue()
        {
            _rabbitMqHistory.Init();
            SetupDefaultExchange();

            PushDummyMigrationHistoryMessage();
            var result = _rabbitMqHistory.GetAllAppliedMigrations();

            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.AllMigrations.Count);
            var migration = result.AllMigrations.First();
            Assert.AreEqual("test", migration.Prefix);
            Assert.AreEqual(1, migration.AppliedMigrations.Count);
            Assert.AreEqual("001_TestMigration", migration.AppliedMigrations.First().Name);
        }

        [TestMethod]
        public void TestGetAppliedMigrations()
        {
            _rabbitMqHistory.Init();
            SetupDefaultExchange();

            PushDummyMigrationHistoryMessage();
            var result = _rabbitMqHistory.GetAppliedMigrations("test");

            Assert.IsNotNull(result);
            Assert.AreEqual("test", result.Prefix);
            Assert.AreEqual(1, result.AppliedMigrations.Count);
            Assert.AreEqual("001_TestMigration", result.AppliedMigrations.First().Name);
        }

        [TestMethod]
        public void TestGetAppliedMigrations2()
        {
            _rabbitMqHistory.Init();
            SetupDefaultExchange();

            PushDummyMigrationHistoryMessage();
            var result = _rabbitMqHistory.GetAppliedMigrations("dummy");

            Assert.IsNotNull(result);
            Assert.AreEqual("dummy", result.Prefix);
            Assert.AreEqual(0, result.AppliedMigrations.Count);
        }

        private void SetupDefaultExchange()
        {
            using (var connection = _connectionFactory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare("", ExchangeType.Direct, true);
                channel.QueueBind(Constants.HistoryQueue, "", Constants.HistoryQueue, null);
            }
        }

        private void PushDummyMigrationHistoryMessage()
        {
            var migrationHistoryRow = new MigrationHistoryRow { Prefix = "test" };
            migrationHistoryRow.AppliedMigrations.Add("001_TestMigration");
            var migrationHistory = new MigrationHistory();
            migrationHistory.AllMigrations.Add(migrationHistoryRow);

            using (var connection = _connectionFactory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                var messageBody = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(migrationHistory));
                channel.BasicPublish("", Constants.HistoryQueue, false, null, messageBody);
            }
        }
    }
}