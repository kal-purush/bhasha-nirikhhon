using ShortSharing.API.Dtos.ThingDtos;

namespace ShortSharing.Tests;

public static class SeedData
{
    public static CreateThingDto GetValidCreateThingDto()
    {
        return new CreateThingDto
        {
            Name = "Thing",
            Description = "valid description",
            Price = 100.0,
            CategoryId = Guid.NewGuid(),
            TypeId = Guid.NewGuid()
        };
    }

    public static CreateThingDto GetInvalidCreateThingDto_MissingName()
    {
        return new CreateThingDto
        {
            Name = string.Empty,
            Description = "valid description",
            Price = 100.0,
            CategoryId = Guid.NewGuid(),
            TypeId = Guid.NewGuid()
        };
    }

    public static CreateThingDto GetInvalidCreateThingDto_NegativePrice()
    {
        return new CreateThingDto
        {
            Name = "Thing",
            Description = "valid description",
            Price = -10.0,
            CategoryId = Guid.NewGuid(),
            TypeId = Guid.NewGuid()
        };
    }

    public static CreateThingDto GetInvalidCreateThingDto_EmptyDescription()
    {
        return new CreateThingDto
        {
            Name = "Thing",
            Description = "",
            Price = 100.0,
            CategoryId = Guid.NewGuid(),
            TypeId = Guid.NewGuid()
        };
    }
    public static CreateThingDto GetInvalidCreateThingDto_InvalidTypeId()
    {
        return new CreateThingDto
        {
            Name = "Thing",
            Description = "",
            Price = 100.0,
            CategoryId = Guid.NewGuid(),
            TypeId = Guid.Empty
        };
    }
    public static CreateThingDto GetInvalidCreateThingDto_InvalidCategoryId()
    {
        return new CreateThingDto
        {
            Name = "Thing",
            Description = "",
            Price = 100.0,
            CategoryId = Guid.Empty,
            TypeId = Guid.NewGuid()
        };
    }
}