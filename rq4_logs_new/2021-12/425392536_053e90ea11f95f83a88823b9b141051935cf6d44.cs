using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;

namespace Parithon.StreamDeck.SDK.Messages
{
  public class SendToPropertyInspectorMessage : IMessage
  {
    public SendToPropertyInspectorMessage(string action, string context, dynamic payload)
    {
      this.Action = action;
      this.Context = context;
      this.Payload = payload;
    }

    [JsonProperty("action")]
    public string Action { get; set; }

    [JsonProperty("event")]
    public string Event => "sendToPropertyInspector";

    [JsonProperty("context")]
    public string Context { get; private set; }

    [JsonProperty("payload")]
    public dynamic Payload { get; private set; }
  }
}