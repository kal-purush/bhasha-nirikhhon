using CSharpFunctionalExtensions;
using PetFamily.Domain.Shared;

namespace PetFamily.Domain.Entities.Pets;

public record PetDetails
{
    private PetDetails(
        double weight, 
        double height, 
        bool isCastrated, 
        bool isVaccinated, 
        string color, 
        string healthInfo,
        DateTime birthday)
    {
        Weight = weight;
        Height = height;
        IsCastrated = isCastrated;
        IsVaccinated = isVaccinated;
        Color = color;
        HealthInfo = healthInfo;
        Birthday = birthday;
    }

    public double Weight { get; }
    
    public double Height { get; }
    
    public bool IsCastrated { get; }
    
    public bool IsVaccinated { get; }
    
    public string Color { get; }
    
    public string HealthInfo { get; }
    
    public DateTime Birthday { get; }

    public static Result<PetDetails, Error> Create(
        double weight,
        double height,
        bool isCastrated,
        bool isVaccinated,
        string color,
        string healthInfo,
        DateTime birthday)
    {
        if(weight <= 0)
            return Errors.General.ValueIsInvalid(nameof(weight));
        
        if (height <= 0)
            return Errors.General.ValueIsInvalid(nameof(height));
        
        if(string.IsNullOrWhiteSpace(color) || color.Length > Constants.MAX_LOW_TEXT_LENGTH)
            return Errors.General.ValueIsInvalid(nameof(color));
        
        if (string.IsNullOrWhiteSpace(healthInfo) || healthInfo.Length > Constants.Pet.MAX_HEALTH_INFO_LENGTH)
            return Errors.General.ValueIsInvalid(nameof(healthInfo));
        
        if(birthday > DateTime.Now - TimeSpan.FromDays(Constants.Pet.MAX_PET_AGE * 365))
            return Errors.General.ValueIsInvalid(nameof(birthday));

        var petDetails = new PetDetails(
            weight, 
            height, 
            isCastrated, 
            isVaccinated, 
            color, 
            healthInfo, 
            birthday
            );

        return petDetails;
    }
}