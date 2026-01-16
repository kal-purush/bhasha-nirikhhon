using System;
using Newtonsoft.Json;

namespace Visibility.Lambda.Slack
{
    public class CloudWatchEvent<TDetail>
    {
        public string Version { get; set; }
        public Guid Id { get; set; }
        [JsonProperty("detail-type")]
        public string DetailType { get; set; }
        public string Source { get; set; }
        public string Time { get; set; }
        public string Account { get; set; }
        public string Region { get; set; }
        public string[] Resources { get; set; }
        public TDetail Detail { get; set; }
    }
}