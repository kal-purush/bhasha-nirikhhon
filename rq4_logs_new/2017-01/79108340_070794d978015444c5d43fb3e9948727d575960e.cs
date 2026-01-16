using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace AutoShutdown
{
    public class SlackField {
        [JsonProperty("title")]
        public string Title { get; set; }
        [JsonProperty("value")]
        public string Value { get; set; }
        [JsonProperty("short")]
        public bool Short { get; set; }
    }
    public class SlackMessage {
        [JsonProperty("fallback")]
        public string Fallback { get; set; }
        [JsonProperty("text")]
        public string Text { get; set; }
        [JsonProperty("pretext")]
        public string PreText { get; set; }
        [JsonProperty("color")]
        public string Color { get; set; }
        [JsonProperty("channel")]
        public string Channel {get;set;}
        [JsonProperty("username")]
        public string Username {get;set;}
        [JsonProperty("fields")]
        public List<SlackField> Fields { get; set; }
        [JsonProperty("mrkdwn_in")]
        public List<string> MarkdownIn{
            get {
                return new List<string>(){"text","pretext"};
            }
        }
    }	
    public class SlackPayload {
        public SlackPayload()
        {
            Attachments = new List<SlackMessage>();
        }
        [JsonProperty("attachments")]
        public List<SlackMessage> Attachments {get;set;}
    }
    public class SlackClient
    {
        private readonly Uri _webhookUrl;
        private readonly HttpClient _httpClient = new HttpClient();
    
        public SlackClient(Uri webhookUrl)
        {
            _webhookUrl = webhookUrl;
        }
    
        public async Task<HttpResponseMessage> SendMessageAsync(string message,
            string channel = null, string username = null)
        {
            var payload = new SlackMessage()
            {
                Text = message,
                Channel = channel,
                Username = username
            };
            var serializedPayload = JsonConvert.SerializeObject(payload);
            var response = await _httpClient.PostAsync(_webhookUrl,
                new StringContent(serializedPayload, Encoding.UTF8, "application/json"));
    
            return response;
        }
        public async Task<HttpResponseMessage> SendMessageAsync(SlackPayload payload)
        {
            var serializedPayload = JsonConvert.SerializeObject(payload);
            var response = await _httpClient.PostAsync(_webhookUrl,
                new StringContent(serializedPayload, Encoding.UTF8, "application/json"));
            return response;
        }
    }
}