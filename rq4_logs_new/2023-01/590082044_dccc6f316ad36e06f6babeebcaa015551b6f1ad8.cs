using CraftHouse.Web.Entities;

namespace CraftHouse.Web.DTOs;

public class RegisterUserDto
{
    public string FirstName { get; init; } = null!;
    public string LastName { get; init; } = null!;
    public string TelephoneNumber { get; init; } = null!;
    public string City { get; init; } = null!;
    public string PostalCode { get; init; } = null!;
    public string AddressLine { get; init; } = null!;
    public string Mail { get; init; } = null!;
    public string Password { get; init; } = null!;
    public string ConfirmPassword { get; init; } = null!;

    public User MapToUser()
    {
        var user = new User()
        {
            FirstName = FirstName,
            LastName = LastName,
            TelephoneNumber = TelephoneNumber,
            City = City,
            PostalCode = PostalCode,
            AddressLine = AddressLine,
            Email = Mail,
        };

        return user;
    }
    
}