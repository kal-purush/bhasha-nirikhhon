using Autofac;
using MongoDB.Driver;
using System.Configuration;

namespace CCT.Infrastructure.DI
{
    public class InfrastructureModule : Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            RegisterMongoDb(builder);
        }

        private void RegisterMongoDb(ContainerBuilder builder) {
            var client = new MongoClient(ConfigurationManager.AppSettings["DATABASE_CONNECTION_STRING"]);
            builder.Register(x => client.GetDatabase(ConfigurationManager.AppSettings["DATABASE_NAME"])).InstancePerRequest();
        }
    }
}