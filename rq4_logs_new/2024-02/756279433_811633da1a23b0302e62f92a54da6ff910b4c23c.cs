using AskHand.Domain.Levels.Exceptions;
using System.Text.RegularExpressions;

namespace AskHand.Domain.Levels;

public sealed partial class Level : ValueObject
{
    [GeneratedRegex(@"^4(?:[a-cA-C](?:\+|\b)?)?|[5-9][a-cA-C](?:\+|\b)")]
    private static partial Regex RateRegex();

    public string Rate { get; }

    private Level(string rate)
    {
        if (!RateRegex().IsMatch(rate))
        {
            throw new RateFormatException();
        }

        Rate = rate;
    }

    public override IEnumerable<object> GetEqualityComponents()
    {
        yield return Rate;
    }

    public static Level Create(string rate)
    {
        return new Level(rate);
    }
}