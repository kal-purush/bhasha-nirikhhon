using AskHand.Domain.Countries.Exceptions;

namespace AskHand.Domain.Countries;

public sealed class Country : ValueObject
{
    public string Name { get; }

    private Country(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new CountryMissingArgumentException("Country name is missing");
        }

        Name = name;
    }

    public static Country Create(string name)
    {
        return new Country(name);
    }

    public override IEnumerable<object> GetEqualityComponents()
    {
        yield return Name;
    }
}