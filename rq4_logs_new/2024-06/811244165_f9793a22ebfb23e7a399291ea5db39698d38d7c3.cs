using HealthMate.API.Abstractions;
using HealthMate.API.ViewModels.Activity;
using HealthMate.API.ViewModels.Gender;
using HealthMate.API.ViewModels.Health;
using HealthMate.API.ViewModels.Medication;
using HealthMate.API.ViewModels.Mood;
using HealthMate.API.ViewModels.Nutrition;

namespace HealthMate.API.ViewModels.User
{
    public class UserViewModel(
        string Name,
        string UserName,
        string Email,
        DateTime DateOfBirth,
        GenderViewModel Gender,
        double Height,
        double Weight,

        ICollection<HealthViewModel>? HealthCollection,
        ICollection<ActivityViewModel>? ActivityCollection,
        ICollection<NutritionViewModel>? NutritionCollection,
        ICollection<MedicationViewModel>? MedicationsCollection,
        ICollection<MoodViewModel>? MoodsCollection
    ) : BaseDto;
}