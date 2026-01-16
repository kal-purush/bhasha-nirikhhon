using Newtonsoft.Json;

namespace Narochno.Slack.Entities.Requests
{
    public class OAuthAccessRequest : BaseRequest
    {
        [JsonProperty("client_id", Required = Required.Always)]
        public string ClientId { get; set; }
        [JsonProperty("client_secret", Required = Required.Always)]
        public string ClientSecret { get; set; }
        [JsonProperty("code", Required = Required.Always)]
        public string Code { get; set; }
        [JsonProperty("redirect_uri", DefaultValueHandling = DefaultValueHandling.Ignore)]
        public string RedirectUri { get; set; }
    }
}