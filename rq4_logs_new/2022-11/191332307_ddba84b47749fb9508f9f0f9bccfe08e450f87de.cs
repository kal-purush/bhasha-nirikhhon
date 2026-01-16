namespace HolisticWare.Xamarin.Tools.NuGet.Client.ServerAPI.Generated.PackageRegistration
{
    using System;

    using Newtonsoft.Json;

    public partial class Root
    {
        [JsonProperty("@id", NullValueHandling = NullValueHandling.Ignore)]
        public Uri Id { get; set; }

        [JsonProperty("@type", NullValueHandling = NullValueHandling.Ignore)]
        public string[] Type { get; set; }

        [JsonProperty("commitId", NullValueHandling = NullValueHandling.Ignore)]
        public Guid? CommitId { get; set; }

        [JsonProperty("commitTimeStamp", NullValueHandling = NullValueHandling.Ignore)]
        public DateTimeOffset? CommitTimeStamp { get; set; }

        [JsonProperty("count", NullValueHandling = NullValueHandling.Ignore)]
        public long? Count { get; set; }

        [JsonProperty("items", NullValueHandling = NullValueHandling.Ignore)]
        public CommitItems[] Items { get; set; }

        [JsonProperty("@context", NullValueHandling = NullValueHandling.Ignore)]
        public Context Context { get; set; }
    }

    public partial class Context
    {
        [JsonProperty("@vocab", NullValueHandling = NullValueHandling.Ignore)]
        public Uri Vocab { get; set; }

        [JsonProperty("catalog", NullValueHandling = NullValueHandling.Ignore)]
        public Uri Catalog { get; set; }

        [JsonProperty("xsd", NullValueHandling = NullValueHandling.Ignore)]
        public Uri Xsd { get; set; }

        [JsonProperty("items", NullValueHandling = NullValueHandling.Ignore)]
        public Dependencies Items { get; set; }

        [JsonProperty("commitTimeStamp", NullValueHandling = NullValueHandling.Ignore)]
        public CommitTimeStamp CommitTimeStamp { get; set; }

        [JsonProperty("commitId", NullValueHandling = NullValueHandling.Ignore)]
        public CommitId CommitId { get; set; }

        [JsonProperty("count", NullValueHandling = NullValueHandling.Ignore)]
        public CommitId Count { get; set; }

        [JsonProperty("parent", NullValueHandling = NullValueHandling.Ignore)]
        public CommitTimeStamp Parent { get; set; }

        [JsonProperty("tags", NullValueHandling = NullValueHandling.Ignore)]
        public Dependencies Tags { get; set; }

        [JsonProperty("reasons", NullValueHandling = NullValueHandling.Ignore)]
        public Reasons Reasons { get; set; }

        [JsonProperty("packageTargetFrameworks", NullValueHandling = NullValueHandling.Ignore)]
        public Dependencies PackageTargetFrameworks { get; set; }

        [JsonProperty("dependencyGroups", NullValueHandling = NullValueHandling.Ignore)]
        public Dependencies DependencyGroups { get; set; }

        [JsonProperty("dependencies", NullValueHandling = NullValueHandling.Ignore)]
        public Dependencies Dependencies { get; set; }

        [JsonProperty("packageContent", NullValueHandling = NullValueHandling.Ignore)]
        public PackageContent PackageContent { get; set; }

        [JsonProperty("published", NullValueHandling = NullValueHandling.Ignore)]
        public PackageContent Published { get; set; }

        [JsonProperty("registration", NullValueHandling = NullValueHandling.Ignore)]
        public PackageContent Registration { get; set; }
    }

    public partial class CommitId
    {
        [JsonProperty("@id", NullValueHandling = NullValueHandling.Ignore)]
        public string Id { get; set; }
    }

    public partial class CommitTimeStamp
    {
        [JsonProperty("@id", NullValueHandling = NullValueHandling.Ignore)]
        public string Id { get; set; }

        [JsonProperty("@type", NullValueHandling = NullValueHandling.Ignore)]
        public string Type { get; set; }
    }

    public partial class Dependencies
    {
        [JsonProperty("@id", NullValueHandling = NullValueHandling.Ignore)]
        public string Id { get; set; }

        [JsonProperty("@container", NullValueHandling = NullValueHandling.Ignore)]
        public string Container { get; set; }
    }

    public partial class PackageContent
    {
        [JsonProperty("@type", NullValueHandling = NullValueHandling.Ignore)]
        public string Type { get; set; }
    }

    public partial class Reasons
    {
        [JsonProperty("@container", NullValueHandling = NullValueHandling.Ignore)]
        public string Container { get; set; }
    }

    public partial class CommitItems
    {
        [JsonProperty("@id", NullValueHandling = NullValueHandling.Ignore)]
        public Uri Id { get; set; }

        [JsonProperty("@type", NullValueHandling = NullValueHandling.Ignore)]
        public string Type { get; set; }

        [JsonProperty("commitId", NullValueHandling = NullValueHandling.Ignore)]
        public Guid? CommitId { get; set; }

        [JsonProperty("commitTimeStamp", NullValueHandling = NullValueHandling.Ignore)]
        public DateTimeOffset? CommitTimeStamp { get; set; }

        [JsonProperty("count", NullValueHandling = NullValueHandling.Ignore)]
        public long? Count { get; set; }

        [JsonProperty("items", NullValueHandling = NullValueHandling.Ignore)]
        public ItemItem[] Items { get; set; }

        [JsonProperty("parent", NullValueHandling = NullValueHandling.Ignore)]
        public Uri Parent { get; set; }

        [JsonProperty("lower", NullValueHandling = NullValueHandling.Ignore)]
        public string Lower { get; set; }

        [JsonProperty("upper", NullValueHandling = NullValueHandling.Ignore)]
        public string Upper { get; set; }
    }

    public partial class ItemItem
    {
        [JsonProperty("@id", NullValueHandling = NullValueHandling.Ignore)]
        public Uri Id { get; set; }

        [JsonProperty("@type", NullValueHandling = NullValueHandling.Ignore)]
        public string Type { get; set; }

        [JsonProperty("commitId", NullValueHandling = NullValueHandling.Ignore)]
        public Guid? CommitId { get; set; }

        [JsonProperty("commitTimeStamp", NullValueHandling = NullValueHandling.Ignore)]
        public DateTimeOffset? CommitTimeStamp { get; set; }

        [JsonProperty("catalogEntry", NullValueHandling = NullValueHandling.Ignore)]
        public CatalogEntry CatalogEntry { get; set; }

        [JsonProperty("packageContent", NullValueHandling = NullValueHandling.Ignore)]
        public Uri PackageContent { get; set; }

        [JsonProperty("registration", NullValueHandling = NullValueHandling.Ignore)]
        public Uri Registration { get; set; }
    }

    public partial class CatalogEntry
    {
        [JsonProperty("@id", NullValueHandling = NullValueHandling.Ignore)]
        public Uri Id { get; set; }

        [JsonProperty("@type", NullValueHandling = NullValueHandling.Ignore)]
        public string Type { get; set; }

        [JsonProperty("authors", NullValueHandling = NullValueHandling.Ignore)]
        public string Authors { get; set; }

        [JsonProperty("dependencyGroups", NullValueHandling = NullValueHandling.Ignore)]
        public DependencyGroup[] DependencyGroups { get; set; }

        [JsonProperty("description", NullValueHandling = NullValueHandling.Ignore)]
        public string Description { get; set; }

        [JsonProperty("iconUrl", NullValueHandling = NullValueHandling.Ignore)]
        public Uri IconUrl { get; set; }

        [JsonProperty("id", NullValueHandling = NullValueHandling.Ignore)]
        public string CatalogEntryId { get; set; }

        [JsonProperty("language", NullValueHandling = NullValueHandling.Ignore)]
        public string Language { get; set; }

        [JsonProperty("licenseExpression", NullValueHandling = NullValueHandling.Ignore)]
        public string LicenseExpression { get; set; }

        [JsonProperty("licenseUrl", NullValueHandling = NullValueHandling.Ignore)]
        public Uri LicenseUrl { get; set; }

        [JsonProperty("listed", NullValueHandling = NullValueHandling.Ignore)]
        public bool? Listed { get; set; }

        [JsonProperty("minClientVersion", NullValueHandling = NullValueHandling.Ignore)]
        public string MinClientVersion { get; set; }

        [JsonProperty("packageContent", NullValueHandling = NullValueHandling.Ignore)]
        public Uri PackageContent { get; set; }

        [JsonProperty("projectUrl", NullValueHandling = NullValueHandling.Ignore)]
        public Uri ProjectUrl { get; set; }

        [JsonProperty("published", NullValueHandling = NullValueHandling.Ignore)]
        public DateTimeOffset? Published { get; set; }

        [JsonProperty("requireLicenseAcceptance", NullValueHandling = NullValueHandling.Ignore)]
        public bool? RequireLicenseAcceptance { get; set; }

        [JsonProperty("summary", NullValueHandling = NullValueHandling.Ignore)]
        public string Summary { get; set; }

        [JsonProperty("tags", NullValueHandling = NullValueHandling.Ignore)]
        public string[] Tags { get; set; }

        [JsonProperty("title", NullValueHandling = NullValueHandling.Ignore)]
        public string Title { get; set; }

        [JsonProperty("version", NullValueHandling = NullValueHandling.Ignore)]
        public string Version { get; set; }
    }

    public partial class DependencyGroup
    {
        [JsonProperty("@id", NullValueHandling = NullValueHandling.Ignore)]
        public Uri Id { get; set; }

        [JsonProperty("@type", NullValueHandling = NullValueHandling.Ignore)]
        public string Type { get; set; }

        [JsonProperty("dependencies", NullValueHandling = NullValueHandling.Ignore)]
        public Dependency[] Dependencies { get; set; }

        [JsonProperty("targetFramework", NullValueHandling = NullValueHandling.Ignore)]
        public string TargetFramework { get; set; }
    }

    public partial class Dependency
    {
        [JsonProperty("@id", NullValueHandling = NullValueHandling.Ignore)]
        public Uri Id { get; set; }

        [JsonProperty("@type", NullValueHandling = NullValueHandling.Ignore)]
        public string Type { get; set; }

        [JsonProperty("id", NullValueHandling = NullValueHandling.Ignore)]
        public string DependencyId { get; set; }

        [JsonProperty("range", NullValueHandling = NullValueHandling.Ignore)]
        public string Range { get; set; }

        [JsonProperty("registration", NullValueHandling = NullValueHandling.Ignore)]
        public Uri Registration { get; set; }
    }
}

