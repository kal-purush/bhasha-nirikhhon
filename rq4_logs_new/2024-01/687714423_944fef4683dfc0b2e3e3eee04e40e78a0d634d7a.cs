using Azure;
using EmbeddingEngine.Core;
using Microsoft.SemanticKernel;
using Microsoft.SemanticKernel.Embeddings;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;

namespace EmbeddingEngine.HuggingFace
{
    public class RetryHandler : DelegatingHandler
    {
        private const int MaxRetries = 3;

        public RetryHandler(HttpMessageHandler innerHandler)
            : base(innerHandler)
        { }

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            HttpResponseMessage response = null;
            for (int i = 0; i < MaxRetries; i++)
            {
                response = await base.SendAsync(request, cancellationToken);
                if (response.IsSuccessStatusCode)
                {
                    return response;
                }
            }

            return response;
        }
    }

    public class HuggingFaceEngine : IEmbeddingEngine
    {
        public string m_providerKey { get; set; }
        public string m_modelName { get; set; }
        public EmbeddingsProviders provider { get; } = EmbeddingsProviders.HUGGING_FACE;

        public HuggingFaceEngine(string providerKey,
                                 string modelName)
        {
            m_providerKey = providerKey;
            m_modelName = modelName;
        }

        public string ModelName
        {
            get
            {
                return m_modelName;
            }
        }

        public async Task<T?> GenerateEmbeddingsAsync<T>(string input)
        {
            try
            {
                using HttpClient httpClient = new HttpClient();
                httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", m_providerKey);

                HttpRequestMessage requestMessage = new HttpRequestMessage(HttpMethod.Post,
                                                            $"https://api-inference.huggingface.co/pipeline/feature-extraction/{m_modelName}");
                requestMessage.Content = JsonContent.Create($"query: {input}");
                HttpResponseMessage responseMessage = await httpClient.SendAsync(requestMessage);
                responseMessage.EnsureSuccessStatusCode();

                var body = await responseMessage.Content.ReadAsStringAsync();
                var embeddingResponse = JsonSerializer.Deserialize<T>(body);
                return embeddingResponse ?? default;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return default;
            }
        }

        public async Task<float[]?> GenerateEmbeddingsAsync(string input)
        {
            using HttpClient httpClient = new HttpClient(new RetryHandler(new HttpClientHandler()));
            httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", m_providerKey);

            HttpRequestMessage requestMessage = new(HttpMethod.Post,
                                                    $"https://api-inference.huggingface.co/pipeline/feature-extraction/{m_modelName}");
            requestMessage.Content = JsonContent.Create($"query: {input}");
            HttpResponseMessage responseMessage = await httpClient.SendAsync(requestMessage);
            responseMessage.EnsureSuccessStatusCode();

            var body = await responseMessage.Content.ReadAsStringAsync();
            try
            {
                var embeddingResponse = JsonSerializer.Deserialize<float[]>(body);
                return embeddingResponse ?? Array.Empty<float>();
            }
            catch (Exception ex)
            {
                var embeddingResponse = JsonSerializer.Deserialize<float[][][]>(body);
                return embeddingResponse[0][0] ?? Array.Empty<float>();
            }

            return Array.Empty<float>();

            //IKernelBuilder kernelBuilder = Kernel.CreateBuilder();
            //kernelBuilder.Services.AddLogging(c => c.AddConsole());
            //kernelBuilder.AddHuggingFaceTextEmbeddingGeneration(model_id, httpClient: httpClient);

            //var kernel = kernelBuilder.Build();

            //var service = kernel.GetRequiredService<ITextEmbeddingGenerationService>();
            //var _embeddings = await service.GenerateEmbeddingsAsync(["how old are you"]);
        }
    }
}