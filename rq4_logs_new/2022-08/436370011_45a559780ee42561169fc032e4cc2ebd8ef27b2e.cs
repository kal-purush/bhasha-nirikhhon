using System.ComponentModel.DataAnnotations;
using System.Text.Json.Serialization;

using JetBrains.Annotations;

namespace Zastai.NuGet.Server.Controllers.Documents;

/// <summary>A set of versions available for a package.</summary>
[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
public class PackageVersions {

  /// <summary>Creates a new set of package versions.</summary>
  /// <param name="versions">The available versions for the package.</param>
  public PackageVersions(IReadOnlyList<string> versions) {
    this.Versions = versions;
  }

  /// <summary>The available versions for the package.</summary>
  [JsonPropertyName("versions")]
  [Required]
  public IReadOnlyList<string> Versions { get; }

}