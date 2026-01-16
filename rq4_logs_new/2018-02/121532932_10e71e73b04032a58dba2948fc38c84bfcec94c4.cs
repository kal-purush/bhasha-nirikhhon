// To parse this JSON data, add NuGet 'Newtonsoft.Json' then do:
//
//    using GithubWebhook;
//
//    var data = MembershipEvent.FromJson(jsonString);

namespace GithubWebhook
{
    using System;
    using System.Net;
    using System.Collections.Generic;

    using Newtonsoft.Json;


    public partial class MembershipEvent
    {
        public static MembershipEvent FromJson(string json) => JsonConvert.DeserializeObject<MembershipEvent>(json, GithubWebhook.Converter.Settings);
    }

    public static class Serialize
    {
        public static string ToJson(this MembershipEvent self) => JsonConvert.SerializeObject(self, GithubWebhook.Converter.Settings);
    }

    public partial class Converter
    {
        public static readonly JsonSerializerSettings Settings = new JsonSerializerSettings
        {
            MetadataPropertyHandling = MetadataPropertyHandling.Ignore,
            DateParseHandling = DateParseHandling.None,
        };
    }
}