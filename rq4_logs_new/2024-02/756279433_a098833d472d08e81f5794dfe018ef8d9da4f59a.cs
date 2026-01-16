using AskHand.Domain.Emails.Exceptions;
using System.Text.RegularExpressions;

namespace AskHand.Domain.Emails;

public sealed partial class Email : ValueObject
{
    [GeneratedRegex(@"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")]
    private static partial Regex EmailRegex();

    public string Value { get; }

    public Email(string value)
    {
        if (!EmailRegex().IsMatch(value))
        {
            throw new EmailFormatException();
        }

        Value = value;
    }

    public static Email Create(string email)
    {
        return new Email(email);
    }

    public override IEnumerable<object> GetEqualityComponents()
    {
        yield return Value;
    }
}