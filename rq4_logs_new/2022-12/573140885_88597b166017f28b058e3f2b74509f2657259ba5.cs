#nullable disable

using System.Text.Json.Serialization;

namespace Models;

public class Address
{
    [JsonPropertyName("postalCode")]
    public string PostalCode { get; set; }

    [JsonPropertyName("street")]
    public string Street { get; set; }

    [JsonPropertyName("neighborhood")]
    public string Neighborhood { get; set; }

    [JsonPropertyName("state")]
    public string State { get; set; }

    [JsonPropertyName("municipalCode")]
    public int MunicipalCode { get; set; }

    [JsonPropertyName("countryCode")]
    public int CountryCode { get; set; }
}