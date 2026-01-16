using Falu.Core;
using System.Text.Json.Serialization;

namespace Falu.Client.MoneyStatements;

public class MoneyStatement : IHasId, IHasCreated, IHasUpdated, IHasWorkspace, IHasLive, IHasEtag
{
    /// <inheritdoc/>
    public string? Id { get; set; }

    /// <inheritdoc/>
    public DateTimeOffset Created { get; set; }

    /// <inheritdoc/>
    public DateTimeOffset Updated { get; set; }

    /// <summary>The provider of statement.</summary>
    public string? Provider { get; set; }

    /// <summary>
    /// The kind of objects that the statement contains.
    /// </summary>
    [JsonPropertyName("objects_kind")]
    public string? ObjectsKind { get; set; }

    /// <summary>
    /// Whether the statement was uploaded by a workspace user.
    /// These statements cannot be downloaded later.
    /// </summary>
    public bool Uploaded { get; set; }

    /// <summary>Type of file.</summary>
    public string? Type { get; set; }

    /// <summary>
    /// A name of the file suitable for saving to a file system.
    /// </summary>
    public string? Filename { get; set; }

    /// <summary>Size in bytes of the file.</summary>
    public long Size { get; set; }

    /// <inheritdoc/>
    public string? Workspace { get; set; }

    /// <inheritdoc/>
    public bool Live { get; set; }

    /// <inheritdoc/>
    public string? Etag { get; set; }
}