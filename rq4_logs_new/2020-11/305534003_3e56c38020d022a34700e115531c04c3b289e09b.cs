using System;
using System.Collections.Generic;
using System.Globalization;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace CriticsReviews
{
    using System;
    using System.Collections.Generic;

    using System.Globalization;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Converters;

    public partial class CriticsReview
    {
        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("favoritegenre")]
        public string Favoritegenre { get; set; }

        [JsonProperty("criticName")]
        public CriticName[] CriticName { get; set; }

        [JsonProperty("moviereviewed")]
        public string Moviereviewed { get; set; }

        [JsonProperty("review")]
        public string Review { get; set; }
    }

    public partial class CriticName
    {
        [JsonProperty("firstName")]
        public string FirstName { get; set; }

        [JsonProperty("lastName")]
        public string LastName { get; set; }
    }

    public partial class CriticsReview
    {
        public static CriticsReview[] FromJson(string json) => JsonConvert.DeserializeObject<CriticsReview[]>(json, CriticsReviews.Converter.Settings);
    }

    public static class Serialize
    {
        public static string ToJson(this CriticsReview[] self) => JsonConvert.SerializeObject(self, CriticsReviews.Converter.Settings);
    }

    internal static class Converter
    {
        public static readonly JsonSerializerSettings Settings = new JsonSerializerSettings
        {
            MetadataPropertyHandling = MetadataPropertyHandling.Ignore,
            DateParseHandling = DateParseHandling.None,
            Converters =
            {
                new IsoDateTimeConverter { DateTimeStyles = DateTimeStyles.AssumeUniversal }
            },
        };
    }
}