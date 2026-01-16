namespace HolisticWare.Xamarin.Tools.NuGet.Client.ServerAPI.Generated.PackageRegistration
{
    using System;

    using Jil;

    public partial class Root
    {
        public Uri Id { get; set; }

        public string[] Type { get; set; }

        public Guid? CommitId { get; set; }

        public DateTimeOffset? CommitTimeStamp { get; set; }

        public long? Count { get; set; }

        public CommitItems[] Items { get; set; }

        public Context Context { get; set; }
    }

    public partial class Context
    {
        public Uri Vocab { get; set; }

        public Uri Catalog { get; set; }

        public Uri Xsd { get; set; }

        public Dependencies Items { get; set; }

        public CommitTimeStamp CommitTimeStamp { get; set; }

        public CommitId CommitId { get; set; }

        public CommitId Count { get; set; }

        public CommitTimeStamp Parent { get; set; }

        public Dependencies Tags { get; set; }

        public Reasons Reasons { get; set; }

        public Dependencies PackageTargetFrameworks { get; set; }

        public Dependencies DependencyGroups { get; set; }

        public Dependencies Dependencies { get; set; }

        public PackageContent PackageContent { get; set; }

        public PackageContent Published { get; set; }

        public PackageContent Registration { get; set; }
    }

    public partial class CommitId
    {
        public string Id { get; set; }
    }

    public partial class CommitTimeStamp
    {
        public string Id { get; set; }

        public string Type { get; set; }
    }

    public partial class Dependencies
    {
        public string Id { get; set; }

        public string Container { get; set; }
    }

    public partial class PackageContent
    {
        public string Type { get; set; }
    }

    public partial class Reasons
    {
        public string Container { get; set; }
    }

    public partial class CommitItems
    {
        public Uri Id { get; set; }

        public string Type { get; set; }

        public Guid? CommitId { get; set; }

        public DateTimeOffset? CommitTimeStamp { get; set; }

        public long? Count { get; set; }

        public ItemItem[] Items { get; set; }

        public Uri Parent { get; set; }

        public string Lower { get; set; }

        public string Upper { get; set; }
    }

    public partial class ItemItem
    {
        public Uri Id { get; set; }

        public string Type { get; set; }

        public Guid? CommitId { get; set; }

        public DateTimeOffset? CommitTimeStamp { get; set; }

        public CatalogEntry CatalogEntry { get; set; }

        public Uri PackageContent { get; set; }

        public Uri Registration { get; set; }
    }

    public partial class CatalogEntry
    {
        public Uri Id { get; set; }

        public string Type { get; set; }

        public string Authors { get; set; }

        public DependencyGroup[] DependencyGroups { get; set; }

        public string Description { get; set; }

        public Uri IconUrl { get; set; }

        public string CatalogEntryId { get; set; }

        public string Language { get; set; }

        public string LicenseExpression { get; set; }

        public Uri LicenseUrl { get; set; }

        public bool? Listed { get; set; }

        public string MinClientVersion { get; set; }

        public Uri PackageContent { get; set; }

        public Uri ProjectUrl { get; set; }

        public DateTimeOffset? Published { get; set; }

        public bool? RequireLicenseAcceptance { get; set; }

        public string Summary { get; set; }

        public string[] Tags { get; set; }

        public string Title { get; set; }

        public string Version { get; set; }
    }

    public partial class DependencyGroup
    {
        public Uri Id { get; set; }

        public string Type { get; set; }

        public Dependency[] Dependencies { get; set; }

        public string TargetFramework { get; set; }
    }

    public partial class Dependency
    {
        public Uri Id { get; set; }

        public string Type { get; set; }

        public string DependencyId { get; set; }

        public string Range { get; set; }

        public Uri Registration { get; set; }
    }
}

