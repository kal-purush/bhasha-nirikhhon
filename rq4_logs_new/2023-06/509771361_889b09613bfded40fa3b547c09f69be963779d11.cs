using FoodBackend.Domain.Foodstuffs;
using FoodBackend.UseCases.Common.Dtos;
using GymBackend.Infrastructure.Abstractions.Interfaces;

namespace FoodBackend.UseCases.CourseMeal;

/// <summary>
/// Count course meal characteristics sum service.
/// </summary>
public class CountCourseMealCharacteristics : ICountCourseMealCharacteristics
{
    private readonly ICreateCharacteristicsList createCharacteristicsList;
    private readonly ICountRecipeCharacteristics countRecipeCharacteristics;

    /// <summary>
    /// Constructor.
    /// </summary>
    public CountCourseMealCharacteristics(ICreateCharacteristicsList createCharacteristicsList, 
        ICountRecipeCharacteristics countRecipeCharacteristics)
    {
        this.createCharacteristicsList = createCharacteristicsList;
        this.countRecipeCharacteristics = countRecipeCharacteristics;
    }

    /// <inheritdoc />
    public async Task<ICollection<FoodCharacteristicSumDto>> CountCourseMealCharacteristicsSum(DetailCourseMealDto courseMeal, CancellationToken cancellationToken)
    {
        var characteristicsSum = await createCharacteristicsList.CreateCharacteristics(cancellationToken);
        foreach (var foodRecipe in courseMeal.ConsumedRecipes)
        {
            var foodRecipeCharacteristicsSum = await countRecipeCharacteristics
                .CountRecipeCharacteristicSum(foodRecipe.FoodRecipe, cancellationToken);
            foodRecipe.FoodRecipe.CharacteristicsSum = foodRecipeCharacteristicsSum;
            foreach (var characteristic in foodRecipeCharacteristicsSum)
            {
                var increasingCharacteristic = characteristicsSum
                    .FirstOrDefault(dto => dto.FoodCharacteristicType.Id == characteristic.FoodCharacteristicType.Id);
                if (increasingCharacteristic != null)
                {
                    characteristicsSum.Remove(increasingCharacteristic);
                    increasingCharacteristic = increasingCharacteristic with
                    {
                        CharacteristicSumValue = increasingCharacteristic.CharacteristicSumValue +
                                                 characteristic.CharacteristicSumValue / FoodWeightDefaults.DefaultProductValueWeight * foodRecipe.RecipeInMealWeight
                    };
                    characteristicsSum.Add(increasingCharacteristic);
                }
            }
        }
        foreach (var foodElementary in courseMeal.ConsumedElementaries)
        {
            foreach (var characteristic in foodElementary.FoodElementary.Characteristics)
            {
                var increasingCharacteristic = characteristicsSum
                    .FirstOrDefault(dto => dto.FoodCharacteristicType.Id == characteristic.CharacteristicTypeId);
                if (increasingCharacteristic != null)
                {
                    characteristicsSum.Remove(increasingCharacteristic);
                    increasingCharacteristic = increasingCharacteristic with
                    {
                        CharacteristicSumValue = increasingCharacteristic.CharacteristicSumValue +
                                                 characteristic.Value / FoodWeightDefaults.DefaultProductValueWeight * foodElementary.ElementaryInMealWeight
                    };
                    characteristicsSum.Add(increasingCharacteristic);
                }
            }
        }
        return characteristicsSum;
    }
}