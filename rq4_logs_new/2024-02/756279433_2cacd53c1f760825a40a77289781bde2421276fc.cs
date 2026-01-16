using AskHand.Domain.Passwords.Exceptions;
using System.Text.RegularExpressions;

namespace AskHand.Domain.Passwords;

public sealed partial class Password : ValueObject
{
    [GeneratedRegex(@"^(?=.*[A-Za-z0-9])(?=.*[!@#$%^&*()-_+=~`[\]{}|:;""'<>,.?/])(.{10,})$")]
    private static partial Regex PasswordRegex();

    public string Value { get; }

    private Password(string value)
    {
        if (!PasswordRegex().IsMatch(value))
        {
            throw new PasswordFormatException();
        }

        Value = value;
    }

    public static Password Create(string value)
    {
        return new Password(value);
    }

    public override IEnumerable<object> GetEqualityComponents()
    {
        yield return Value;
    }
}