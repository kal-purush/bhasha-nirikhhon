namespace Goal.Shared.Results;

public record DatabaseError(string Message, Exception? Exception = null);