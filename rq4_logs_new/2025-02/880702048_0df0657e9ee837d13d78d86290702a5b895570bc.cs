namespace P2Project.Core.Dtos.VolunteerRequests;

public class VolunteerRequestDto
{
    public Guid RequestId { get; init; }
    public Guid? AdminId { get; init; }
    public Guid UserId { get; init; }
    public string FirstName { get; init; } = string.Empty;
    public string SecondName { get; init; } = string.Empty;
    public string? LastName { get; init; } = string.Empty;
    public int Age { get; init; }
    public int Grade { get; init; }
    public Guid? DiscussionId { get; init; }
    public string Status { get; init; } = string.Empty;
    public DateTime CreatedAt { get; init; }
    public string? RejectionComment { get; init; }
}