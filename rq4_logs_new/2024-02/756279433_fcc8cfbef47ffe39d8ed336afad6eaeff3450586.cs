using AskHand.Domain.ZipCodes.Exceptions;
using System.Text.RegularExpressions;

namespace AskHand.Domain.ZipCodes;

public sealed partial class ZipCode : ValueObject
{
    [GeneratedRegex(@"^\d{5}(?:-\d{4})?$")]
    private static partial Regex ZipCodeRegex();

    public string Value { get; }

    private ZipCode(string value)
    {
        if (string.IsNullOrWhiteSpace(value) || !ZipCodeRegex().IsMatch(value))
        {
            throw new ZipCodeFormatException();
        }

        Value = value;
    }

    public override IEnumerable<object> GetEqualityComponents()
    {
        yield return Value;
    }

    public static ZipCode Create(string value)
    {
        return new ZipCode(value);
    }
}