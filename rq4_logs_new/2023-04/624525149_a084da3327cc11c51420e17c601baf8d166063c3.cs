using FluentValidation.TestHelper;
using TG.Backend.Features.Offer.GetOffers;
using TG.Backend.Features.Offer.Validation;
using TG.Backend.Models.Offer;

namespace TG.Backend.Tests.ValidatorTests.Offer;

public class GetOfferFilterDTOValidatorTests
{
    [Fact]
    public void Validate_ForInvalidModel_ReturnsFailureForGearbox()
    {
        // arrange
        var getOffersFilterDto = new GetOffersFilterDTO
        {
            Gearbox = "abc",
            Drive = "RWD",
            PriceLow = null,
            PriceHigh = null
        };
        var getOffersQuery = new GetOffersQuery(getOffersFilterDto);
        var validator = new GetOffersFilterDTOValidator();
        
        // act
        var result = validator.TestValidate(getOffersQuery);

        // assert
        result.ShouldHaveValidationErrorFor(a => a.Filter.Gearbox);
    }
    
    [Fact]
    public void Validate_ForInvalidModel_ReturnsFailureForDrive()
    {
        // arrange
        var getOffersFilterDto = new GetOffersFilterDTO
        {
            Gearbox = "Automatic",
            Drive = "cds",
            PriceLow = null,
            PriceHigh = null
        };
        var getOffersQuery = new GetOffersQuery(getOffersFilterDto);
        var validator = new GetOffersFilterDTOValidator();
        
        // act
        var result = validator.TestValidate(getOffersQuery);

        // assert
        result.ShouldHaveValidationErrorFor(a => a.Filter.Drive);
    }
    
    [Fact]
    public void Validate_ForInvalidModel_ReturnsFailureForDriveAndGearbox()
    {
        // arrange
        var getOffersFilterDto = new GetOffersFilterDTO
        {
            Gearbox = "fsf",
            Drive = "cds",
            PriceLow = null,
            PriceHigh = null
        };
        var getOffersQuery = new GetOffersQuery(getOffersFilterDto);
        var validator = new GetOffersFilterDTOValidator();
        
        // act
        var result = validator.TestValidate(getOffersQuery);

        // assert
        result.ShouldHaveValidationErrorFor(a => a.Filter.Drive);
        result.ShouldHaveValidationErrorFor(a => a.Filter.Gearbox);
    }
    
    [Theory]
    [MemberData(nameof(GetValidModels))]
    public void Validate_ForValidModel_ReturnsSuccess(GetOffersFilterDTO getOffersFilterDto)
    {
        // arrange
        var getOffersQuery = new GetOffersQuery(getOffersFilterDto);
        var validator = new GetOffersFilterDTOValidator();
        
        // act
        var result = validator.TestValidate(getOffersQuery);

        // assert
        result.ShouldNotHaveAnyValidationErrors();
    }


    public static IEnumerable<object[]> GetValidModels()
    {
        var models = new List<GetOffersFilterDTO>
        {
            new GetOffersFilterDTO
            {
                Gearbox = "Automatic",
                Drive = "AWD",
                PriceLow = null,
                PriceHigh = null
            },
            new GetOffersFilterDTO
            {
                Gearbox = "Manual",
                Drive = "RWD",
                PriceLow = null,
                PriceHigh = null
            },
            new GetOffersFilterDTO
            {
                Gearbox = "Manual",
                Drive = "FWD",
                PriceLow = null,
                PriceHigh = null
            },
            new GetOffersFilterDTO
            {
                Gearbox = "Automatic",
                Drive = "FWD",
                PriceLow = null,
                PriceHigh = null
            }
        };

        return models.Select(q => new object[] { q });
    }
}