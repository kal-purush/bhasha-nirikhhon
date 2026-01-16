namespace FoodBackend.UseCases.Common.Dtos;

/// <summary>
/// Food recipe dto with characteristics sum.
/// </summary>
public record FoodRecipeDtoWithCharacteristics
{
    /// <summary>
    /// Food recipe id.
    /// </summary>
    public Guid Id { get; init; }

    /// <summary>
    /// Food recipe name.
    /// </summary>
    public string Name { get; init; }
    
    /// <summary>
    /// Id of user who created recipe.
    /// </summary>
    public Guid? UserId { get; init; }

    /// <summary>
    /// Ingredients collection.
    /// </summary>
    public ICollection<FoodElementaryInRecipeDto> Ingredients { get; init; }
    
    /// <summary>
    /// Food recipe characteristics sum information.
    /// </summary>
    public ICollection<FoodRecipeCharacteristicSumDto> CharacteristicsSum { get; init; }
}