namespace WebApiDapperNativeAOT.Models.Requests.Todo;

public record TodoSearchRequest
(
    string[]? Title,
    string[]? Description,
    int? CreatedBy,
    int[]? AssignedTo,
    DateTime[]? TargetDate,
    bool? IsComplete
);