using System.ComponentModel.DataAnnotations;
using System.Text.Json.Serialization;

using JetBrains.Annotations;

using Zastai.NuGet.Server.Controllers.Api;

namespace Zastai.NuGet.Server.Controllers.Documents;

/// <summary>A NuGet v3 service index document.</summary>
[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
public sealed class ServiceIndex {

  private const string SchemaVersion = "3.0.0";

  /// <summary>The schema version for this service index document.</summary>
  [JsonPropertyName("version")]
  [Required]
  public string Version { get; } = ServiceIndex.SchemaVersion;

  /// <summary>The resources defined in the service index.</summary>
  [JsonPropertyName("resources")]
  [Required]
  public IEnumerable<ServiceIndexResource> Resources { get; }

  /// <summary>Creates a new NuGet v3 service index document.</summary>
  /// <param name="baseUri">The base URI for resources with a relative path.</param>
  public ServiceIndex(Uri baseUri) {
    var resources = new List<ServiceIndexResource>();
    ServiceIndex.Add(resources, baseUri, Autocomplete.Service);
    ServiceIndex.Add(resources, baseUri, Catalog.Service);
    ServiceIndex.Add(resources, baseUri, PackageDownload.Service);
    ServiceIndex.Add(resources, baseUri, PackageDetails.Service);
    ServiceIndex.Add(resources, baseUri, PackageMetadata.Service);
    ServiceIndex.Add(resources, baseUri, PublishPackage.Service);
    ServiceIndex.Add(resources, baseUri, PublishSymbols.Service);
    ServiceIndex.Add(resources, baseUri, ReportAbuse.Service);
    ServiceIndex.Add(resources, baseUri, Api.RepositorySignatures.Service);
    ServiceIndex.Add(resources, baseUri, Search.Service);
    ServiceIndex.Add(resources, baseUri, SymbolServer.Service);
    this.Resources = resources;
  }

  private static void Add(ICollection<ServiceIndexResource> resources, Uri baseUri, NuGetService service) {
    foreach (var type in service.Types) {
      resources.Add(new ServiceIndexResource(new Uri(baseUri, service.Path), type, service.Description));
    }
  }

}