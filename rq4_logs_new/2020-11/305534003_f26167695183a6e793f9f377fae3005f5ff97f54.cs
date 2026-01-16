using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace IMDB
{
    using System;
    using System.Collections.Generic;

    using System.Globalization;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Converters;

    public partial class MovieImdb
    {
        [JsonProperty("id")]
        public object Id { get; set; }

        [JsonProperty("title")]
        public object Title { get; set; }

        [JsonProperty("originalTitle")]
        public object OriginalTitle { get; set; }

        [JsonProperty("fullTitle")]
        public object FullTitle { get; set; }

        [JsonProperty("type")]
        public object Type { get; set; }

        [JsonProperty("year")]
        public object Year { get; set; }

        [JsonProperty("image")]
        public object Image { get; set; }

        [JsonProperty("releaseDate")]
        public object ReleaseDate { get; set; }

        [JsonProperty("runtimeMins")]
        public object RuntimeMins { get; set; }

        [JsonProperty("runtimeStr")]
        public object RuntimeStr { get; set; }

        [JsonProperty("plot")]
        public object Plot { get; set; }

        [JsonProperty("plotLocal")]
        public object PlotLocal { get; set; }

        [JsonProperty("plotLocalIsRtl")]
        public bool PlotLocalIsRtl { get; set; }

        [JsonProperty("awards")]
        public object Awards { get; set; }

        [JsonProperty("directors")]
        public object Directors { get; set; }

        [JsonProperty("directorList")]
        public object DirectorList { get; set; }

        [JsonProperty("writers")]
        public object Writers { get; set; }

        [JsonProperty("writerList")]
        public object WriterList { get; set; }

        [JsonProperty("stars")]
        public object Stars { get; set; }

        [JsonProperty("starList")]
        public object StarList { get; set; }

        [JsonProperty("actorList")]
        public object ActorList { get; set; }

        [JsonProperty("fullCast")]
        public object FullCast { get; set; }

        [JsonProperty("genres")]
        public object Genres { get; set; }

        [JsonProperty("genreList")]
        public object GenreList { get; set; }

        [JsonProperty("companies")]
        public object Companies { get; set; }

        [JsonProperty("companyList")]
        public object CompanyList { get; set; }

        [JsonProperty("countries")]
        public object Countries { get; set; }

        [JsonProperty("countryList")]
        public object CountryList { get; set; }

        [JsonProperty("languages")]
        public object Languages { get; set; }

        [JsonProperty("languageList")]
        public object LanguageList { get; set; }

        [JsonProperty("contentRating")]
        public object ContentRating { get; set; }

        [JsonProperty("imDbRating")]
        public object ImDbRating { get; set; }

        [JsonProperty("imDbRatingVotes")]
        public object ImDbRatingVotes { get; set; }

        [JsonProperty("metacriticRating")]
        public object MetacriticRating { get; set; }

        [JsonProperty("ratings")]
        public object Ratings { get; set; }

        [JsonProperty("wikipedia")]
        public object Wikipedia { get; set; }

        [JsonProperty("posters")]
        public object Posters { get; set; }

        [JsonProperty("images")]
        public object Images { get; set; }

        [JsonProperty("trailer")]
        public object Trailer { get; set; }

        [JsonProperty("boxOffice")]
        public object BoxOffice { get; set; }

        [JsonProperty("tagline")]
        public object Tagline { get; set; }

        [JsonProperty("keywords")]
        public object Keywords { get; set; }

        [JsonProperty("keywordList")]
        public object KeywordList { get; set; }

        [JsonProperty("similars")]
        public object Similars { get; set; }

        [JsonProperty("tvSeriesInfo")]
        public object TvSeriesInfo { get; set; }

        [JsonProperty("tvEpisodeInfo")]
        public object TvEpisodeInfo { get; set; }

        [JsonProperty("errorMessage")]
        public string ErrorMessage { get; set; }
    }

    public partial class MovieImdb
    {
        public static MovieImdb FromJson(string json) => JsonConvert.DeserializeObject<MovieImdb>(json, IMDB.Converter.Settings);
    }

    public static class Serialize
    {
        public static string ToJson(this MovieImdb self) => JsonConvert.SerializeObject(self, IMDB.Converter.Settings);
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