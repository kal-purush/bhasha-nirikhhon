using System.ComponentModel.DataAnnotations;
using System.Text.Json.Serialization;

using JetBrains.Annotations;

namespace Zastai.NuGet.Server.Controllers.Documents;

/// <summary>A set of fingerprints for a signing certificate.</summary>
[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
public class CertificateFingerprints {

  /// <summary>Creates a new set of fingerprints for a signing certificate.</summary>
  /// <param name="sha256">The SHA-256 fingerprint.</param>
  public CertificateFingerprints(string sha256) {
    this.SHA256 = sha256;
  }

  /// <summary>The SHA-256 fingerprint.</summary>
  [JsonPropertyName("2.16.840.1.101.3.4.2.1")]
  [Required]
  public string SHA256 { get; }

}