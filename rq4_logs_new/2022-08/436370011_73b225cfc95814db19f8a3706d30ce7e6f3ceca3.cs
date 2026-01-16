namespace Zastai.NuGet.Server.Auth;

/// <summary>The role names for access to NuGet operations.</summary>
public static class NuGetRoles {

  /// <summary>A role that is allowed to delete/unlist/relist packages.</summary>
  public const string CanDeletePackages = nameof(NuGetRoles.CanDeletePackages);

  /// <summary>A role that is allowed to publish packages.</summary>
  public const string CanPublishPackages = nameof(NuGetRoles.CanPublishPackages);

  /// <summary>A role that is allowed to publish symbols.</summary>
  public const string CanPublishSymbols = nameof(NuGetRoles.CanPublishSymbols);

}