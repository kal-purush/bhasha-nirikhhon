namespace FoodBackend.UseCases.Common.Dtos;

/// <summary>
/// Course meal day dto with detail information.
/// </summary>
public record DetailCourseMealDayDto
{
    /// <summary>
    /// Course meal day id.
    /// </summary>
    public Guid Id { get; init; }
    
    /// <summary>
    /// Id of user who created course meal day.
    /// </summary>
    public Guid UserId { get; init; }

    /// <summary>
    /// Course meal date.
    /// </summary>
    public DateOnly CourseMealDate { get; init; }

    /// <summary>
    /// Course meals for current day.
    /// </summary>
    public ICollection<DetailCourseMealDto> CourseMeals { get; init; }

    /// <summary>
    /// Course meal day food characteristics sum values.
    /// </summary>
    public ICollection<FoodCharacteristicSumDto> CharacteristicsSum { get; set; }
}