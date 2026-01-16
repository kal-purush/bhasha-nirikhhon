using EmbeddingEngine.Core;
using RestSharp;
using System.Text.Json;

namespace EmbeddingEngine.AlephAlpha
{
    public class AlephAlphaResponse
    {
        public string model_version { get; set; }
        public float[] embedding { get; set; }
    }

    public class AlephAlphaEngine : IEmbeddingEngine
    {
        public string m_providerKey { get; set; }
        public string m_modelName { get; set; }
        public EmbeddingsProviders provider { get; } = EmbeddingsProviders.ALEPH_ALPHA;

        public string ModelName
        {
            get
            {
                return m_modelName;
            }
        }

        public AlephAlphaEngine(string providerKey,
                    string modelName)
        {
            m_providerKey = providerKey;
            m_modelName = modelName;
        }

        public async Task<float[]?> GenerateEmbeddingsAsync(string input)
        {
            var options = new RestClientOptions()
            {
                MaxTimeout = -1,
            };

            var client = new RestClient();
            var request = new RestRequest("https://api.aleph-alpha.com/semantic_embed", Method.Post);

            request.AddHeader("Content-Type", "application/json");
            request.AddHeader("Accept", "application/json");
            request.AddHeader("Authorization", $"Bearer {m_providerKey}");

            // The default behavior is to return the full embedding with 5120 dimensions.
            // With this parameter you can compress the returned embedding to 128 dimensions.
            // The compression is expected to result in a small drop in accuracy performance (4-6%),
            // with the benefit of being much smaller, which makes comparing these embeddings much faster for use cases
            // where speed is critical.
            // With the compressed embedding can also perform better if you are embedding really short texts or documents.
            var body = @"{" + "\n" +
            $@"  ""model"": ""{m_modelName}""," + "\n" +
            $@"  ""prompt"": ""{input}""," + "\n" +
            @"  ""representation"": ""symmetric""," + "\n" +
            @"  ""normalize"": true," + "\n" +
            @"  ""compress_to_size"": 128" + "\n" +
            @"}";

            request.AddStringBody(body, DataFormat.Json);

            RestResponse response = await client.ExecuteAsync(request);
            var aaResponse = JsonSerializer.Deserialize<AlephAlphaResponse>(response.Content);
            return aaResponse.embedding;
        }

        public Task<T?> GenerateEmbeddingsAsync<T>(string input)
        {
            throw new NotImplementedException();
        }
    }
}