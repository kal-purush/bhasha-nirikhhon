using System;
using Newtonsoft.Json;

namespace GitHubCommunicationService.Data.Models
{
    public record Repository
    {
        [JsonProperty("id")] public long Id { get; init; }

        [JsonProperty("node_id")] public string? NodeId { get; init; }

        [JsonProperty("name")] public string? Name { get; init; }

        [JsonProperty("full_name")] public string? FullName { get; init; }

        [JsonProperty("private")] public bool Private { get; init; }

        [JsonProperty("owner")] public RepositoryOwner? Owner { get; init; }

        [JsonProperty("html_url")] public Uri? HtmlUrl { get; init; }

        [JsonProperty("description")] public string? Description { get; init; }

        [JsonProperty("fork")] public bool Fork { get; init; }

        [JsonProperty("url")] public Uri? Url { get; init; }

        [JsonProperty("forks_url")] public Uri? ForksUrl { get; init; }

        [JsonProperty("keys_url")] public string? KeysUrl { get; init; }

        [JsonProperty("collaborators_url")] public string? CollaboratorsUrl { get; init; }

        [JsonProperty("teams_url")] public Uri? TeamsUrl { get; init; }

        [JsonProperty("hooks_url")] public Uri? HooksUrl { get; init; }

        [JsonProperty("issue_events_url")] public string? IssueEventsUrl { get; init; }

        [JsonProperty("events_url")] public Uri? EventsUrl { get; init; }

        [JsonProperty("assignees_url")] public string? AssigneesUrl { get; init; }

        [JsonProperty("branches_url")] public string? BranchesUrl { get; init; }

        [JsonProperty("tags_url")] public Uri? TagsUrl { get; init; }

        [JsonProperty("blobs_url")] public string? BlobsUrl { get; init; }

        [JsonProperty("git_tags_url")] public string? GitTagsUrl { get; init; }

        [JsonProperty("git_refs_url")] public string? GitRefsUrl { get; init; }

        [JsonProperty("trees_url")] public string? TreesUrl { get; init; }

        [JsonProperty("statuses_url")] public string? StatusesUrl { get; init; }

        [JsonProperty("languages_url")] public Uri? LanguagesUrl { get; init; }

        [JsonProperty("stargazers_url")] public Uri? StargazersUrl { get; init; }

        [JsonProperty("contributors_url")] public Uri? ContributorsUrl { get; init; }

        [JsonProperty("subscribers_url")] public Uri? SubscribersUrl { get; init; }

        [JsonProperty("subscription_url")] public Uri? SubscriptionUrl { get; init; }

        [JsonProperty("commits_url")] public string? CommitsUrl { get; init; }

        [JsonProperty("git_commits_url")] public string? GitCommitsUrl { get; init; }

        [JsonProperty("comments_url")] public string? CommentsUrl { get; init; }

        [JsonProperty("issue_comment_url")] public string? IssueCommentUrl { get; init; }

        [JsonProperty("contents_url")] public string? ContentsUrl { get; init; }

        [JsonProperty("compare_url")] public string? CompareUrl { get; init; }

        [JsonProperty("merges_url")] public Uri? MergesUrl { get; init; }

        [JsonProperty("archive_url")] public string? ArchiveUrl { get; init; }

        [JsonProperty("downloads_url")] public Uri? DownloadsUrl { get; init; }

        [JsonProperty("issues_url")] public string? IssuesUrl { get; init; }

        [JsonProperty("pulls_url")] public string? PullsUrl { get; init; }

        [JsonProperty("milestones_url")] public string? MilestonesUrl { get; init; }

        [JsonProperty("notifications_url")] public string? NotificationsUrl { get; init; }

        [JsonProperty("labels_url")] public string? LabelsUrl { get; init; }

        [JsonProperty("releases_url")] public string? ReleasesUrl { get; init; }

        [JsonProperty("deployments_url")] public Uri? DeploymentsUrl { get; init; }

        [JsonProperty("created_at")] public DateTimeOffset CreatedAt { get; init; }

        [JsonProperty("updated_at")] public DateTimeOffset UpdatedAt { get; init; }

        [JsonProperty("pushed_at")] public DateTimeOffset PushedAt { get; init; }

        [JsonProperty("git_url")] public string? GitUrl { get; init; }

        [JsonProperty("ssh_url")] public string? SshUrl { get; init; }

        [JsonProperty("clone_url")] public Uri? CloneUrl { get; init; }

        [JsonProperty("svn_url")] public Uri? SvnUrl { get; init; }

        [JsonProperty("homepage")] public string? Homepage { get; init; }

        [JsonProperty("size")] public long Size { get; init; }

        [JsonProperty("stargazers_count")] public long StargazersCount { get; init; }

        [JsonProperty("watchers_count")] public long WatchersCount { get; init; }

        [JsonProperty("language")] public string? Language { get; init; }

        [JsonProperty("has_issues")] public bool HasIssues { get; init; }

        [JsonProperty("has_projects")] public bool HasProjects { get; init; }

        [JsonProperty("has_downloads")] public bool HasDownloads { get; init; }

        [JsonProperty("has_wiki")] public bool HasWiki { get; init; }

        [JsonProperty("has_pages")] public bool HasPages { get; init; }

        [JsonProperty("forks_count")] public long ForksCount { get; init; }

        [JsonProperty("mirror_url")] public object? MirrorUrl { get; init; }

        [JsonProperty("archived")] public bool Archived { get; init; }

        [JsonProperty("disabled")] public bool Disabled { get; init; }

        [JsonProperty("open_issues_count")] public long OpenIssuesCount { get; init; }

        [JsonProperty("license")] public object? License { get; init; }

        [JsonProperty("allow_forking")] public bool AllowForking { get; init; }

        [JsonProperty("is_template")] public bool IsTemplate { get; init; }

        [JsonProperty("topics")] public object[]? Topics { get; init; }

        [JsonProperty("visibility")] public string? Visibility { get; init; }

        [JsonProperty("forks")] public long Forks { get; init; }

        [JsonProperty("open_issues")] public long OpenIssues { get; init; }

        [JsonProperty("watchers")] public long Watchers { get; init; }

        [JsonProperty("default_branch")] public string? DefaultBranch { get; init; }

        [JsonProperty("permissions")] public RepositoryPermissions? Permissions { get; init; }
    }
}