using System.Text.Json.Serialization;

namespace Remoteit.Models
{
    public class ServiceConnection
    {
        [JsonPropertyName("connectionid")]
        public string ConnectionId { get; set; }

        [JsonPropertyName("deviceaddress")]
        public string DeviceAddress { get; set; }

        [JsonPropertyName("expirationsec")]
        public string ExpirationSec { get; set; }

        [JsonPropertyName("proxy")]
        public string Proxy { get; set; }

        [JsonPropertyName("proxyport")]
        public string ProxyPort { get; set; }

        [JsonPropertyName("proxyserver")]
        public string ProxyServer { get; set; }
    }
}