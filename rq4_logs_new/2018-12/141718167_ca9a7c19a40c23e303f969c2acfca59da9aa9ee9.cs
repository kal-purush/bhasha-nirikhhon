using Dolittle.Runtime.Events.Relativity;
using Dolittle.Runtime.Events.Relativity.Specs;
using Dolittle.Runtime.Events.Specs.MongoDB;

namespace Dolittle.Runtime.Events.Relativity.Specs.MongoDB
{
    public class SUTProvider : IProvideGeodesics
    {
        public IGeodesics Build() => new test_mongodb_geodesics(new a_mongo_db_connection(), Dolittle.Runtime.Events.Specs.MongoDB.given.a_logger());
    }
}