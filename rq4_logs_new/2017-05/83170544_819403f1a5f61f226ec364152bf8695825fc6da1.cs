using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Slalom.Stacks.Services;
using Slalom.Stacks.Services.Messaging;

namespace Slalom.Stacks.AspNetCore.Events.EndPoints
{
    public class ConsumeEventFeed : EndPoint<ConsumeEventFeedRequest>
    {
        private readonly IMessageGateway _messages;
        private readonly List<string> _received = new List<string>();
        private DateTimeOffset? _start;

        public ConsumeEventFeed(IMessageGateway messages)
        {
            _messages = messages;
        }

        public override void Receive(ConsumeEventFeedRequest instance)
        {
            using (var client = new HttpClient())
            {
                var content = client.GetStringAsync(this.GetUrl(instance)).Result;

                var items = JsonConvert.DeserializeObject<JObject[]>(content);
                foreach (var item in items)
                {
                    var name = item["name"].Value<string>();
                    var id = item["id"].Value<string>();
                    if (!_received.Contains(id))
                    {
                        _received.Add(id);
                        _messages.Publish(name, item.ToString());

                        var last = DateTimeOffset.Parse(item["timeStamp"].Value<string>());
                        if (_start == null || last > _start)
                        {
                            _start = last;
                        }
                    }
                }
            }
        }

        private string GetUrl(ConsumeEventFeedRequest instance)
        {
            var root = instance.Url + "/_system/events";
            if (_start.HasValue)
            {
                root += "?start=" + _start.Value;
            }
            return root;
        }
    }
}