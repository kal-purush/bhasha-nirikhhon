using MongoDB.Driver;

namespace CCT.Infrastructure.Queries
{
    public interface IQuery<out TResult>
    {
        TResult Execute(IMongoDatabase database);
    }

    public interface IQueryDispatcher
    {
        TResult Execute<TResult>(IQuery<TResult> query);
    }

    public class QueryDispatcher : IQueryDispatcher
    {
        private readonly IMongoDatabase _database;

        public QueryDispatcher(IMongoDatabase database)
        {
            _database = database;
        }

        public TResult Execute<TResult>(IQuery<TResult> query)
        {
            return query.Execute(_database);
        }
    }
}