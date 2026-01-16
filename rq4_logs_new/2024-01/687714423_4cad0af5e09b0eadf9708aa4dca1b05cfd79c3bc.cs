using Ardalis.GuardClauses;
using EmbeddingEngine.Core;
using Microsoft.SemanticKernel.Connectors.OpenAI;
using Microsoft.SemanticKernel.Connectors.Qdrant;
using Microsoft.SemanticKernel.Memory;
using Tlv.Search.Common;
using VectorDb.Core;

namespace Tlv.Recall.Services
{
    public class SearchService(string apiKey,
                               string endpoint,
                               string collectionName,
                               string vectorDbProviderKey)
    {
        private readonly string _apiKey = apiKey;
        private readonly string _endpoint = endpoint;
        private readonly string _collectionName = collectionName;
        private readonly string _qdrantHost = vectorDbProviderKey;

        protected async Task<List<SearchItem>> Search(string embeddingsProviderName,
                                                      string prompt,
                                                      ulong limit = 1)
        {
            EmbeddingsProviders embeddingsProvider =
                (EmbeddingsProviders)Enum.Parse(typeof(EmbeddingsProviders), embeddingsProviderName);
            IEmbeddingEngine? embeddingEngine = EmbeddingEngine.Core.EmbeddingEngine.Create(embeddingsProvider,
                                                                                    providerKey: _apiKey);
            Guard.Against.Null(embeddingEngine);

            ReadOnlyMemory<float> promptEmbedding = await embeddingEngine.GenerateEmbeddingsAsync(prompt);

            IVectorDb? vectorDb = VectorDb.Core.VectorDb.Create(VectorDbProviders.QDrant, _qdrantHost);
            Guard.Against.Null(vectorDb);

            return await vectorDb.Search($"{_collectionName}_{embeddingsProviderName}",
                                        promptEmbedding,
                                        limit: limit);
        }

        public async Task<List<SearchItem>> Search(string prompt,
                                                   int limit = 1)
        {
#pragma warning disable SKEXP0003, SKEXP0011, SKEXP0026
            var embeddingService =
                new AzureOpenAITextEmbeddingGenerationService("ada2", _endpoint, _apiKey);
            ISemanticTextMemory memory = new MemoryBuilder()
                                            .WithTextEmbeddingGeneration(embeddingService)
                                            .WithQdrantMemoryStore($"http://{_qdrantHost}:6333", 1536)
                                            .Build();


            var collections = await memory.GetCollectionsAsync();
            if (!collections.Contains(_collectionName))
                return [];

            IAsyncEnumerable<MemoryQueryResult> memories = memory.SearchAsync(_collectionName, prompt, limit: limit);
            var q = from res in memories
                    let metadata = res.Metadata
                    select new SearchItem()
                    {
                        //id = ulong.Parse(res.Metadata.Id),
                        summary = metadata.Text,
                        similarity = res.Relevance,

                    };
            return q.ToListAsync().Result;

#pragma warning restore SKEXP0003, SKEXP0011, SKEXP0026
        }
    }
}