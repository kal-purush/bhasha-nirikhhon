namespace Zastai.NuGet.Server.Services;

/// <summary>An API key.</summary>
public interface IApiKey {

  /// <summary>Indicates whether the key can be used to delete/unlist/relist packages.</summary>
  public bool CanDelete { get; }

  /// <summary>Indicates whether the key can be used to publish packages.</summary>
  public bool CanPublish { get; }

  /// <summary>The creation date for the key.</summary>
  public DateTimeOffset Created { get; }

  /// <summary>The expiry date for the key.</summary>
  public DateTimeOffset Expiry { get; }

  /// <summary>The key value.</summary>
  public string Id { get; }

  /// <summary>A descriptive name for the key.</summary>
  public string Name { get; }

  /// <summary>The key's owner (a user ID).</summary>
  public string Owner { get; }

}