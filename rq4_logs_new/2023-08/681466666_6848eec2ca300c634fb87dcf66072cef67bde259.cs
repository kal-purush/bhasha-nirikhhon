using Elastic.Clients.Elasticsearch;

namespace BetterCoding.MessagePubSubCenter.Repository.ElasticSearch
{
    public interface IElasticSearchRepository<T> : IRepository<T>
    {
        ElasticsearchClient Client { get; }
    }

    public abstract class ElasticSearchRepository<T> : IElasticSearchRepository<T>
    {
        private readonly ElasticsearchClient _client;
        public ElasticsearchClient Client => _client;

        protected ElasticSearchRepository(ElasticsearchClient client)
        {
            _client = client;
        }

        public abstract string GetClassName(T entity);

        public virtual async Task<T> AddAsync(T entity)
        {
            var indexName = GetClassName(entity);
            var response = await _client.IndexAsync(entity, indexName);
            if (!response.IsValidResponse)
            {
                if (response.ApiCallDetails.OriginalException != null)
                    throw response.ApiCallDetails.OriginalException;
                else throw new InvalidOperationException(response.ApiCallDetails.ToString());
            }

            return entity;
        }
    }
}