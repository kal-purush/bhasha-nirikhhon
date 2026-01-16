using Ardalis.GuardClauses;
using EmbeddingEngine.Core;
using Microsoft.SemanticKernel.Connectors.HuggingFace;
using Microsoft.SemanticKernel.Connectors.OpenAI;
using Microsoft.SemanticKernel.Embeddings;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Tlv.Search.Common;
using VectorDb.Core;

namespace Tlv.Recall.Services
{
    public class SearchService(IVectorDb _vectorDb, 
                                IEmbeddingEngine _embeddingEngine,
                                string _collectionName)
    {
        public IVectorDb vectorDb { get; set; } = _vectorDb;
        public IEmbeddingEngine embeddingEngine { get; set; } = _embeddingEngine;
        public string collectionName { get; set; } = _collectionName;

        public async Task<List<SearchItem>> Search(string prompt,
                                                   ulong limit = 1)
        {
            Guard.Against.Null(embeddingEngine);
            Guard.Against.Null(vectorDb);

            ReadOnlyMemory<float> promptEmbedding = await embeddingEngine.GenerateEmbeddingsAsync(prompt);
            return await vectorDb.Search(collectionName,
                                        promptEmbedding,
                                        limit: limit);
        }

    }
}