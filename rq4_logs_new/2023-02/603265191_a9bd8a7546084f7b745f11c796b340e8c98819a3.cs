namespace AzurePipelines.Toolkit.Extensions;

using AzurePipelines.Toolkit.Models;

public static class EnumExtensions
{
    public static string GetText(this ArtifactType artifactType)
    {
        return artifactType switch
        {
            ArtifactType.Container => "container",
            ArtifactType.FilePath => "filepath",
            ArtifactType.GitRef => "gitref",
            ArtifactType.TfvcLabel => "tfvclabel",
            ArtifactType.VersionControl => "versioncontrol",
            _ => throw new ArgumentOutOfRangeException(nameof(artifactType), artifactType, null),
        };
    }

    public static string GetText(this AttachmentType attachmentType)
    {
        return attachmentType switch
        {
            AttachmentType.DiagnosticLog => "DistributedTask.Core.DiagnosticLog",
            AttachmentType.FileAttachment => "DistributedTask.Core.FileAttachment",
            AttachmentType.Log => "DistributedTask.Core.Log",
            AttachmentType.ResultsLog => "Results.Core.Log",
            AttachmentType.Summary => "Distributedtask.Core.Summary",
            _ => throw new ArgumentOutOfRangeException(nameof(attachmentType), attachmentType, null),
        };
    }

    internal static string GetText(this IssueType issueType)
    {
        return issueType switch
        {
            IssueType.Error => "error",
            IssueType.Warning => "warning",
            _ => throw new ArgumentOutOfRangeException(nameof(issueType), issueType, null),
        };
    }

    internal static string GetText(this EndpointField endpointField)
    {
        return endpointField switch
        {
            EndpointField.AuthParameter => "authParameter",
            EndpointField.DataParameter => "dataParameter",
            EndpointField.Url => "url",
            _ => throw new ArgumentOutOfRangeException(nameof(endpointField), endpointField, null),
        };
    }
}