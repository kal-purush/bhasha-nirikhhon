using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace ErniAcademy.Serializers.Contracts;

/// <summary>
/// ISerializer contract for generic serialization/deserialization
/// </summary>
public interface ISerializer
{
    /// <summary>
    /// ContentType of the serialization used. e.g. application/json
    /// </summary>
    string ContentType { get; }

    /// <summary>
    /// Serialize a TItem into a stream
    /// </summary>
    /// <typeparam name="TItem">Generic item</typeparam>
    /// <param name="item">The item to be serialized</param>
    /// <param name="stream">The stream target</param>
    void SerializeToStream<TItem>(TItem item, Stream stream);

    /// <summary>
    /// Serialize a TItem into a stream async
    /// </summary>
    /// <typeparam name="TItem">Generic item</typeparam>
    /// <param name="item">The item to be serialized</param>
    /// <param name="stream">The stream target</param>
    /// <param name="cancellationToken">The cancellation token</param>
    /// <returns>A task that represents the asynchronous write operation</returns>
    Task SerializeToStreamAsync<TItem>(TItem item, Stream stream, CancellationToken cancellationToken = default);

    /// <summary>
    /// Serialize a TItem into a string
    /// </summary>
    /// <typeparam name="TItem">Generic item</typeparam>
    /// <param name="item">The item to be serialized</param>
    /// <returns>string</returns>
    string SerializeToString<TItem>(TItem item);

    /// <summary>
    /// Deserialize from stream into an item
    /// </summary>
    /// <typeparam name="TItem">Generic item</typeparam>
    /// <param name="stream">The imput stream</param>
    /// <returns>TItem</returns>
    TItem DeserializeFromStream<TItem>(Stream stream);

    /// <summary>
    /// Deserialize from string into an item
    /// </summary>
    /// <typeparam name="TItem">Generic item</typeparam>
    /// <param name="item">The string to be deserialized</param>
    /// <returns>TItem</returns>
    TItem DeserializeFromString<TItem>(string item);

    /// <summary>
    /// Deserialize from stream into an item async
    /// </summary>
    /// <typeparam name="TItem">Generic item</typeparam>
    /// <param name="stream">The imput stream</param>
    /// <param name="cancellationToken">The cancellation token</param>
    /// <returns>ValueTask<TItem></returns>
    ValueTask<TItem> DeserializeFromStreamAsync<TItem>(Stream stream, CancellationToken cancellationToken = default);
}