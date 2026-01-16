using FluentValidation.Results;

namespace TShort.Api.Errors;

public sealed class PropertyValidationError : Error
{
    public PropertyValidationError(string propertyName, string message, object? attemptedValue = null)
        : base(message)
    {
        Metadata.Add("PropertyName", propertyName);
        Metadata.Add("AttemptedValue", attemptedValue);
    }

    public ValidationFailure ToValidationFailure()
    {
        return new ValidationFailure(Metadata["PropertyName"] as string, Message, Metadata["AttemptedValue"]);
    }
}