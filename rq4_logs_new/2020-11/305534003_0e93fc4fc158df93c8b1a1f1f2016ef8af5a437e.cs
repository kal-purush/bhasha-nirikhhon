using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SoccerGame
{
    using System;
    using System.Collections.Generic;

    using System.Globalization;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Converters;

    public partial class SoccerGames
    {
        [JsonProperty("GameId")]
        public long GameId { get; set; }

        [JsonProperty("RoundId")]
        public long RoundId { get; set; }

        [JsonProperty("Season")]
        public long Season { get; set; }

        [JsonProperty("SeasonType")]
        public long SeasonType { get; set; }

        [JsonProperty("Group")]
        public object Group { get; set; }

        [JsonProperty("AwayTeamId")]
        public long AwayTeamId { get; set; }

        [JsonProperty("HomeTeamId")]
        public long HomeTeamId { get; set; }

        [JsonProperty("VenueId")]
        public object VenueId { get; set; }

        [JsonProperty("Day")]
        public DateTimeOffset Day { get; set; }

        [JsonProperty("DateTime")]
        public object DateTime { get; set; }

        [JsonProperty("Status")]
        public string Status { get; set; }

        [JsonProperty("Week")]
        public long Week { get; set; }

        [JsonProperty("Period")]
        public string Period { get; set; }

        [JsonProperty("Clock")]
        public object Clock { get; set; }

        [JsonProperty("Winner")]
        public string Winner { get; set; }

        [JsonProperty("VenueType")]
        public object VenueType { get; set; }

        [JsonProperty("AwayTeamKey")]
        public string AwayTeamKey { get; set; }

        [JsonProperty("AwayTeamName")]
        public string AwayTeamName { get; set; }

        [JsonProperty("AwayTeamCountryCode")]
        public string AwayTeamCountryCode { get; set; }

        [JsonProperty("AwayTeamScore")]
        public object AwayTeamScore { get; set; }

        [JsonProperty("AwayTeamScorePeriod1")]
        public object AwayTeamScorePeriod1 { get; set; }

        [JsonProperty("AwayTeamScorePeriod2")]
        public object AwayTeamScorePeriod2 { get; set; }

        [JsonProperty("AwayTeamScoreExtraTime")]
        public object AwayTeamScoreExtraTime { get; set; }

        [JsonProperty("AwayTeamScorePenalty")]
        public object AwayTeamScorePenalty { get; set; }

        [JsonProperty("HomeTeamKey")]
        public string HomeTeamKey { get; set; }

        [JsonProperty("HomeTeamName")]
        public string HomeTeamName { get; set; }

        [JsonProperty("HomeTeamCountryCode")]
        public string HomeTeamCountryCode { get; set; }

        [JsonProperty("HomeTeamScore")]
        public object HomeTeamScore { get; set; }

        [JsonProperty("HomeTeamScorePeriod1")]
        public object HomeTeamScorePeriod1 { get; set; }

        [JsonProperty("HomeTeamScorePeriod2")]
        public object HomeTeamScorePeriod2 { get; set; }

        [JsonProperty("HomeTeamScoreExtraTime")]
        public object HomeTeamScoreExtraTime { get; set; }

        [JsonProperty("HomeTeamScorePenalty")]
        public object HomeTeamScorePenalty { get; set; }

        [JsonProperty("HomeTeamMoneyLine")]
        public object HomeTeamMoneyLine { get; set; }

        [JsonProperty("AwayTeamMoneyLine")]
        public object AwayTeamMoneyLine { get; set; }

        [JsonProperty("DrawMoneyLine")]
        public object DrawMoneyLine { get; set; }

        [JsonProperty("PointSpread")]
        public object PointSpread { get; set; }

        [JsonProperty("HomeTeamPointSpreadPayout")]
        public object HomeTeamPointSpreadPayout { get; set; }

        [JsonProperty("AwayTeamPointSpreadPayout")]
        public object AwayTeamPointSpreadPayout { get; set; }

        [JsonProperty("OverUnder")]
        public object OverUnder { get; set; }

        [JsonProperty("OverPayout")]
        public object OverPayout { get; set; }

        [JsonProperty("UnderPayout")]
        public object UnderPayout { get; set; }

        [JsonProperty("Attendance")]
        public object Attendance { get; set; }

        [JsonProperty("Updated")]
        public DateTimeOffset Updated { get; set; }

        [JsonProperty("UpdatedUtc")]
        public DateTimeOffset UpdatedUtc { get; set; }

        [JsonProperty("GlobalGameId")]
        public long GlobalGameId { get; set; }

        [JsonProperty("GlobalAwayTeamId")]
        public long GlobalAwayTeamId { get; set; }

        [JsonProperty("GlobalHomeTeamId")]
        public long GlobalHomeTeamId { get; set; }

        [JsonProperty("ClockExtra")]
        public object ClockExtra { get; set; }

        [JsonProperty("ClockDisplay")]
        public string ClockDisplay { get; set; }

        [JsonProperty("IsClosed")]
        public bool IsClosed { get; set; }

        [JsonProperty("HomeTeamFormation")]
        public object HomeTeamFormation { get; set; }

        [JsonProperty("AwayTeamFormation")]
        public object AwayTeamFormation { get; set; }

        [JsonProperty("PlayoffAggregateScore")]
        public object PlayoffAggregateScore { get; set; }
    }

    public partial class SoccerGames
    {
        public static SoccerGames[] FromJson(string json) => JsonConvert.DeserializeObject<SoccerGames[]>(json, SoccerGame.Converter.Settings);
    }

    public static class Serialize
    {
        public static string ToJson(this SoccerGames[] self) => JsonConvert.SerializeObject(self, SoccerGame.Converter.Settings);
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