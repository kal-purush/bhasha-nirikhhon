using AskHand.Domain.Addresses.Exceptions;
using AskHand.Domain.Cities;
using AskHand.Domain.Countries;
using AskHand.Domain.ZipCodes;
using System.Text.RegularExpressions;

namespace AskHand.Domain.Addresses;

public sealed partial class Address : ValueObject
{
    [GeneratedRegex(@"^\d+[A-Za-z]?(\s?[\/\-]\s?\d+[A-Za-z]?)?(\s?([A-Za-z]+|\#\d+|\b(?:Front|Rear|Side)\b))?")]
    private static partial Regex NumberRegex();

    public string Number { get; }

    public string Street { get; }

    public ZipCode ZipCode { get; }

    public City City { get; }

    public Country Country { get; }

    private Address(
        string number,
        string street,
        ZipCode zipCode,
        City city,
        Country country)
    {
        if (string.IsNullOrWhiteSpace(number) 
            || string.IsNullOrWhiteSpace(street) 
            || zipCode is null
            || city is null
            || country is null)
        {
            throw new AddressMissingArgumentException("One or more argument is missing");
        }

        if (!NumberRegex().IsMatch(number))
        {
            throw new AddressNumberFormatException();
        }

        Number = number;
        Street = street;
        ZipCode = zipCode;
        City = city;
        Country = country;
    }

    public override IEnumerable<object> GetEqualityComponents()
    {
        yield return Number;
        yield return Street;
        yield return ZipCode;
        yield return City;
        yield return Country;
    }

    public static Address Create(
        string number,
        string street,
        ZipCode zipCode,
        City city,
        Country country)
    {
        return new Address(
            number,
            street,
            ZipCode.Create(zipCode.Value),
            City.Create(city.Name),
            Country.Create(country.Name));
    }
}