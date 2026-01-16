using Newtonsoft.Json;

namespace GithubWebhook.Common
{
    public partial class Asset
    {
        [JsonProperty("url")]
        public string Url { get; set; }

        [JsonProperty("browser_download_url")]
        public string BrowserDownloadUrl { get; set; }

        [JsonProperty("id")]
        public long Id { get; set; }

        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("label")]
        public string Label { get; set; }

        [JsonProperty("state")]
        public string State { get; set; }

        [JsonProperty("content_type")]
        public string ContentType { get; set; }

        [JsonProperty("size")]
        public long Size { get; set; }

        [JsonProperty("download_count")]
        public long DownloadCount { get; set; }

        [JsonProperty("created_at")]
        public System.DateTime CreatedAt { get; set; }

        [JsonProperty("updated_at")]
        public System.DateTime UpdatedAt { get; set; }

        [JsonProperty("uploader")]
        public User Uploader { get; set; }
    }
}