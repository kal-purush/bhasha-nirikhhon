using AskHand.Domain.Cities.Exceptions;

namespace AskHand.Domain.Cities;

public sealed class City : ValueObject
{
    public string Name { get; }

    private City(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new CityMissingArgumentException("City name is missing");
        }

        Name = name;
    }

    public static City Create(string name)
    {
        return new City(name);
    }

    public override IEnumerable<object> GetEqualityComponents()
    {
        yield return Name;
    }
}