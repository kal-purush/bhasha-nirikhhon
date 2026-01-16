using CSharpFunctionalExtensions;
using PetFamily.Domain.Shared;

namespace PetFamily.Domain.Entities.ValueObjects;

public record Description
{
    private Description(string value)
    {
        Value = value;
    }
    
    public string Value { get; }

    public static Result<Description, Error> Create(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return Errors.General.ValueIsInvalid(nameof(Description));

        var description = new Description(value);
        
        return description;
    }
}