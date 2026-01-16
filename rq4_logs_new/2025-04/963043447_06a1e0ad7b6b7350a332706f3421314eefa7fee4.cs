using Newtonsoft.Json;

namespace CluedIn.ExternalSearch.Providers.AzureOpenAI.Models
{
    internal class OpenAiCompletionRequest
    {
        [JsonProperty("prompt")]
        public string? Prompt { get; set; }

        [JsonProperty("max_tokens")]
        public int MaxTokens { get; set; }

        [JsonProperty("temperature")]
        public int Temperature { get; set; }

        [JsonProperty("frequency_penalty")]
        public int FrequencyPenalty { get; set; }

        [JsonProperty("presence_penalty")]
        public int PresencePenalty { get; set; }

        [JsonProperty("top_p")]
        public double TopP { get; set; }

        [JsonProperty("best_of")]
        public int BestOf { get; set; }

        [JsonProperty("stop")]
        public object? Stop { get; set; }
    }
}