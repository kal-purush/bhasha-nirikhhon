using EmbeddingEngine.Core;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;

namespace EmbeddingEngine.Gemini
{
    // Request/Response objects
    class Text
    {
        public string text { get; set; }
    }

    class Content
    {
        public Text[] parts { get; set; }
    }
    class Payload
    {
        public string model { get; set; }
        public string task_type { get; set; }
        public Content content { get; set; }
    }

    class Values
    {
        public float[]? values { get; set; }
    }

    class GeminiResponse
    {
        public Values? embedding { get; set; }
    }

    public class GeminiEngine(string providerKey) : IEmbeddingEngine
    {
        public string? m_providerKey { get; set; } = providerKey;

        public EmbeddingsProviders provider { get; } = EmbeddingsProviders.GEMINI;

        public async Task<float[]?> GenerateEmbeddingsAsync(string input)
        {
            if (string.IsNullOrEmpty(input))
                return null;

            try
            {
                const string modelName = "models/embedding-001";

                using HttpClient httpClient = new();
                httpClient.DefaultRequestHeaders.TryAddWithoutValidation("Content-Type", "application/json; charset=utf-8");
                var payload = new Payload
                {
                    model = modelName,
                    //task_type = "RETRIEVAL_QUERY",
                    content = new Content()
                    {
                        parts = [new Text()
                        {
                            text = input
                        }]
                    }
                };

                var options = new JsonSerializerOptions
                {
                    Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
                };


                string jsonPayload = JsonSerializer.Serialize(payload, options);

                //TO-DO: "Request payload size can't exceeds the limit: 10000 bytes.",

                var content = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
                var url = $"https://generativelanguage.googleapis.com/v1beta/{modelName}:embedContent?key={m_providerKey}";
                HttpResponseMessage response = await httpClient.PostAsync(url, content);

                response.EnsureSuccessStatusCode();

                string respContent = await response.Content.ReadAsStringAsync();
                GeminiResponse? geminiResponse = JsonSerializer.Deserialize<GeminiResponse>(respContent);
                return geminiResponse?.embedding?.values;
            }
            catch (Exception)
            {
                throw;
            }

        }

    }
}