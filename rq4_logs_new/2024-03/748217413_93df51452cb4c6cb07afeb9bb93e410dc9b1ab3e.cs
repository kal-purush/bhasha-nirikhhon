using Map.Domain.Entities;

namespace Map.Platform.Interfaces;
public interface ITravelPlatform
{
    /// <summary>
    /// Add Travel between Steps
    /// </summary>
    /// <param name="origin">Origin Step of travel</param>
    /// <param name="destination">Destination Step of travel</param>
    /// <param name="travel">Travel Entity</param>
    Task AddTravelBetweenStepsAsync(Step origin, Step destination, Travel travel);

    /// <summary>
    /// Remove travel Before Step
    /// </summary>
    /// <param name="step">Step Entity With TravelBefore Include <see cref="IStepPlatform.GetStepByIdAsync(int)"/>/></param>
    Task RemoveTravelBeforeStepAsync(Step step);

    /// /// <summary>
    /// Remove travel After Step
    /// </summary>
    /// <param name="step">Step Entity With TravelAfter Include <see cref="IStepPlatform.GetStepByIdAsync(int)"/>/></param>
    Task RemoveTravelAfterStepAsync(Step step);
}