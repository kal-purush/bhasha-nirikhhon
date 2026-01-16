using EA.Serializers.Contracts;
using System.Text.Json;

namespace EA.Serializers.Json;

public class JsonSerializer : ISerializer
{
    private readonly JsonSerializerOptions _jsonSerializerOptions;

    public JsonSerializer(JsonSerializerOptions? jsonSerializerOptions = null)
    {
        _jsonSerializerOptions = jsonSerializerOptions ?? new JsonSerializerOptions();
    }

    public string ContentType => "application/json";

    public void SerializeToStream<TItem>(TItem item, Stream stream) => SerializeToStreamAsync(item, stream).GetAwaiter().GetResult();

    public async Task SerializeToStreamAsync<TItem>(TItem item, Stream stream, CancellationToken cancellationToken = default)
    {
        await System.Text.Json.JsonSerializer.SerializeAsync<TItem>(stream, item, _jsonSerializerOptions, cancellationToken);
        stream.Position = 0;
    }

    public string SerializeToString<TItem>(TItem item) => System.Text.Json.JsonSerializer.Serialize<TItem>(item, _jsonSerializerOptions);

    public TItem DeserializeFromStream<TItem>(Stream stream) => DeserializeFromStreamAsync<TItem>(stream).GetAwaiter().GetResult();

    public TItem DeserializeFromString<TItem>(string item) => System.Text.Json.JsonSerializer.Deserialize<TItem>(item, _jsonSerializerOptions);

    public ValueTask<TItem> DeserializeFromStreamAsync<TItem>(Stream stream, CancellationToken cancellationToken = default) => System.Text.Json.JsonSerializer.DeserializeAsync<TItem>(stream, _jsonSerializerOptions, cancellationToken);
}