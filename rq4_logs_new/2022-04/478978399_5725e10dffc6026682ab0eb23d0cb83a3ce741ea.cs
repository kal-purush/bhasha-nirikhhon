using EA.Serializers.Contracts;
using MessagePack;
using MessagePack.Resolvers;

namespace EA.Serializers.MessagePack;

public class MessagePackSerializer : ISerializer
{
    public string ContentType => "application/x-net-messagepack";

    private readonly MessagePackSerializerOptions _messagePackSerializerOptions;

    public MessagePackSerializer()
        : this(MessagePackSerializerOptions.Standard
            .WithResolver(
                CompositeResolver.Create(
                    NativeGuidResolver.Instance,
                    NativeDecimalResolver.Instance,
                    NativeDateTimeResolver.Instance,
                    StandardResolver.Instance)))
    {
    }

    public MessagePackSerializer(MessagePackSerializerOptions messagePackSerializerOptions)
    {
        _messagePackSerializerOptions = messagePackSerializerOptions;
    }

    public void SerializeToStream<TItem>(TItem item, Stream stream)
    {
        global::MessagePack.MessagePackSerializer.Serialize(stream, item, _messagePackSerializerOptions);
        stream.Position = 0L;
    }

    public async Task SerializeToStreamAsync<TItem>(TItem item, Stream stream, CancellationToken cancellationToken = default)
    {
        await global::MessagePack.MessagePackSerializer.SerializeAsync(stream, item, _messagePackSerializerOptions, cancellationToken);
        stream.Position = 0L;
    }

    public string SerializeToString<TItem>(TItem item)
    {
        using var stream = new MemoryStream();
        SerializeToStream(item, stream);
        return Convert.ToBase64String(stream.ToArray());
    }

    public TItem DeserializeFromStream<TItem>(Stream stream) => global::MessagePack.MessagePackSerializer.Deserialize<TItem>(stream, _messagePackSerializerOptions);

    public TItem DeserializeFromString<TItem>(string item)
    {
        using var stream = new MemoryStream(Convert.FromBase64String(item));
        return DeserializeFromStream<TItem>(stream);
    }

    public ValueTask<TItem> DeserializeFromStreamAsync<TItem>(Stream stream, CancellationToken cancellationToken = default) => global::MessagePack.MessagePackSerializer.DeserializeAsync<TItem>(stream, _messagePackSerializerOptions, cancellationToken);
}