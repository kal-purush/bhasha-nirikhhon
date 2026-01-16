namespace MainService.Domain.Entities;

public class IssueDomain
{
    public string? Id { get; set; }
    public string Title { get; set; } = string.Empty;
    public string ProjectId { get; set; } = string.Empty;
    public string SprintId { get; set; } = string.Empty;
    public string AssigneeId { get; set; } = string.Empty;
    public string ParentId { get; set; } = string.Empty;
    public string ReporterId { get; set; } = string.Empty;
    public IssueType Type { get; set; }
    public IssueStatus Status { get; set; }
    public IssuePriority Priority { get; set; } = IssuePriority.Medium;

    public string Summary { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public int StoryPoint { get; set; }
    public List<string> Attachments { get; set; } = new();
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
}

public enum IssueType
{
    Bug,
    Task,
    Story,
    Epic
}

public enum IssueStatus
{
    Open,
    InProgress,
    Resolved,
    Closed
}
public enum IssuePriority
{
    Low,
    Medium,
    High,
    Critical
}