using Bogus;
using Domain.Entities;
using Infra;

namespace DataGeneration.Implementations;

public class GeneratorImp : Generator
{
    private readonly ApplicationDbContext _applicationDbContext;
    public GeneratorImp(ApplicationDbContext applicationDbContext) : base()
    {
        _applicationDbContext = applicationDbContext;
    }

    public void Generate(int hotelQuantity)
    {
        var hotelFaker = new Faker<Hotel>()
                .RuleFor(e => e.Name, f => f.Address.City() + " Hotel")
                // .RuleFor(e => e.Latitude, f => f.Address.Latitude())
                // .RuleFor(e => e.Longitude, f => f.Address.Longitude())
                // .RuleFor(e => e.Address, f => f.Address.FullAddress())
                .RuleFor(e => e.CreatedAt, DateTime.Now)
                .RuleFor(e => e.UpdatedAt, DateTime.Now)
                .RuleFor(e => e.Address, f => new Address
                {
                    AddressLine = f.Address.StreetAddress(),
                    PostalCode = f.Address.ZipCode(),
                    Longitude = f.Address.Longitude(),
                    Latitude = f.Address.Latitude(),
                    City = f.Address.City()
                })
            ;
        hotelFaker.Locale = "pt_BR";
        
        var hotels = hotelFaker.Generate(hotelQuantity);
        _applicationDbContext.Hotels.AddRange(hotels);
        _applicationDbContext.SaveChanges();
    }
}