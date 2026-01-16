using System.Text.Json;
using System.Text.Json.Serialization;

namespace ITaaS.Infrastructure.CrossCutting
{
    public static class JsonSerializerSettingsGlobalConfig
    {
        public static JsonSerializerOptions Settings { get; } = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            NumberHandling = JsonNumberHandling.AllowReadingFromString,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            Converters = { 
                
                new JsonStringEnumConverter(JsonNamingPolicy.CamelCase, allowIntegerValues: false) }
        };
    }

    internal class DateTimeConverterNull : JsonConverter<DateTime?>
    {
        public override DateTime? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            => string.IsNullOrWhiteSpace(reader.GetString()) ? ((DateTime?)null) : DateTime.Parse(reader.GetString(), CultureInfo.InvariantCulture);
        public override void Write(Utf8JsonWriter writer, DateTime? value, JsonSerializerOptions options)
            => writer.WriteStringValue(value?.ToString("s"));
    }

    internal class DateTimeConverter : JsonConverter<DateTime>
    {
        public override DateTime Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            => DateTime.ParseExact(reader.GetString(), "dd/MM/yyyy", CultureInfo.InvariantCulture);
        public override void Write(Utf8JsonWriter writer, DateTime value, JsonSerializerOptions options)
            => writer.WriteStringValue(value.ToString("s"));
    }
}