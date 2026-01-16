namespace DirectoryProject.DirectoryService.Domain;

public record Error
{
    public static Error Validation(string code, string message, string? invalidField = null) =>
        new(code, message, ErrorType.Validation, invalidField);
    public static Error NotFound(string code, string message) =>
        new(code, message, ErrorType.NotFound);
    public static Error Failure(string code, string message) =>
        new(code, message, ErrorType.Failure);
    public static Error Conflict(string code, string message) =>
        new(code, message, ErrorType.Conflict);
    public static Error AccessDenied(string code, string message) =>
        new(code, message, ErrorType.AccessDenied);

    public string Code { get; }
    public string Message { get; }
    public ErrorType Type { get; }
    public string? InvalidField { get; }

    // for debugging
    public override string ToString() => $"Type: {Type}. {Code}: {Message}";

    private Error(string code, string message, ErrorType type, string? invalidField = null)
    {
        Code = code;
        Message = message;
        Type = type;
        InvalidField = invalidField;
    }
}

public enum ErrorType
{
    Validation,
    NotFound,
    Failure,
    Conflict,
    AccessDenied,
}