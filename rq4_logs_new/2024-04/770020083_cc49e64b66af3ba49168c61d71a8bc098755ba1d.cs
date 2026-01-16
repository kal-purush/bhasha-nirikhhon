namespace ValueObjects;

public class Address
{
    public string PostalCode { get; }
    public string City { get; }
    public string AddressLine { get; }
    public double Latitude { get; }
    public double Longitude { get; }
}