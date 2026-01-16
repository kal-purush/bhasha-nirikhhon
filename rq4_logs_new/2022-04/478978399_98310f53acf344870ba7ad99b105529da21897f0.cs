using EA.Serializers.Contracts;
using Newtonsoft.Json;

namespace EA.Serializers.NewtonsoftJson;

public class JsonSerializer : ISerializer
{
    private readonly Newtonsoft.Json.JsonSerializer _jsonSerializer;
    private readonly JsonSerializerSettings _jsonSerializerSettings;

    public JsonSerializer(JsonSerializerSettings? jsonSerializerSettings = null)
    {
        _jsonSerializerSettings = jsonSerializerSettings ?? new JsonSerializerSettings();
        _jsonSerializer = Newtonsoft.Json.JsonSerializer.Create(_jsonSerializerSettings);
    }

    public string ContentType => "application/json";

    public void SerializeToStream<TItem>(TItem item, Stream stream) => SerializeToStreamAsync(item, stream).GetAwaiter().GetResult();

    public Task SerializeToStreamAsync<TItem>(TItem item, Stream stream, CancellationToken cancellationToken = default)
    {
        using (StreamWriter writer = new StreamWriter(stream))
        using (JsonTextWriter jsonWriter = new JsonTextWriter(writer))
        _jsonSerializer.Serialize(jsonWriter, item);
        stream.Position = 0;
        return Task.CompletedTask;
    }

    public string SerializeToString<TItem>(TItem item) => JsonConvert.SerializeObject(item, _jsonSerializerSettings);

    public TItem DeserializeFromStream<TItem>(Stream stream) => DeserializeFromStreamAsync<TItem>(stream).GetAwaiter().GetResult();

    public TItem DeserializeFromString<TItem>(string item) => JsonConvert.DeserializeObject<TItem>(item, _jsonSerializerSettings);

    public ValueTask<TItem> DeserializeFromStreamAsync<TItem>(Stream stream, CancellationToken cancellationToken = default)
    {
        TItem item = default;
        using (StreamReader reader = new StreamReader(stream))
        using (JsonTextReader jsonReader = new JsonTextReader(reader))
        {
            item = _jsonSerializer.Deserialize<TItem>(jsonReader); 
        }
        return new ValueTask<TItem>(item);
    }
}