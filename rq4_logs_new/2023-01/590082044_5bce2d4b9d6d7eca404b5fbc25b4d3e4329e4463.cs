using System.Diagnostics;
using CraftHouse.Web.Entities;
using CraftHouse.Web.Validators;

namespace CraftHouse.Tests;

public class UserValidatorTests
{
    [Fact]
    public void UserValidator_ShouldPass_WhenGivenValidInput()
    {
        var user = new User()
        {
            FirstName = "Test",
            LastName = "Test",
            AddressLine = "Test street 4",
            City = "New york",
            TelephoneNumber = "123123123",
            Email = "test@test.com",
            PostalCode = "10-100"
        };
        var validator = new UserValidator();

        var result = validator.Validate(user);
        Assert.True(result.IsValid);
    }
}