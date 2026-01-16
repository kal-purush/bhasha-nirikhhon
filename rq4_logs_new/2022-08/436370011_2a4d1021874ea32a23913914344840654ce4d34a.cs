namespace Zastai.NuGet.Server.Services;

/// <summary>A user.</summary>
public interface IUser {

  /// <summary>Indicates whether the user is allowed to delete/unlist/relist packages.</summary>
  public bool CanDelete { get; }

  /// <summary>Indicates whether the user is allowed to publish packages.</summary>
  public bool CanPublish { get; }

  /// <summary>The creation date/time for the user.</summary>
  public DateTimeOffset Created { get; }

  /// <summary>The user's ID.</summary>
  public string Id { get; }

  /// <summary>The user's full name.</summary>
  public string Name { get; }

}