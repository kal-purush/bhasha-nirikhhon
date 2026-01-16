using Org.BouncyCastle.Asn1.X509;

namespace backend_carhelp.Workshop_management.Domain.Model.Aggregates;

public partial class Vehicle
{
    public int Id { get; }
    public string Plate { get; set; }
    public string Brand { get; set; }
    public string Model { get; set; }
    public string Year { get; set; }
    public string Color { get; set; }
    public string ImageUrl { get; set; }
    public string Mileage { get; set; }

    public Vehicle(string plate, string brand, string model, string year, string color, string imageUrl, string mileage)
    {
        Plate = plate;
        Brand = brand;
        Model = model;
        Year = year;
        Color = color;
        ImageUrl = imageUrl;
        Mileage = mileage;
    }
    
}