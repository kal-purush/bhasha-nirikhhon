using Gym.Domain.Meta;

namespace GymBackend.UseCases.Common.Dtos.Workout;

/// <summary>
/// Set dto.
/// </summary>
public record SetDto
{
    /// <summary>
    /// Id.
    /// </summary>
    public Guid Id { get; set; }

    /// <summary>
    /// Train session Id.
    /// </summary>
    public Guid TrainSessionId { get; set; }

    /// <summary>
    /// Number of repetitions.
    /// </summary>
    public int Reps { get; set; }

    /// <summary>
    /// The weight of the projectile.
    /// </summary>
    public double Weight { get; set; }

    /// <summary>
    /// Indicates that set is done.
    /// </summary>
    public bool IsDone { get; set; }

    /// <summary>
    /// Number.
    /// </summary>
    public int Number { get; set; }

    #region Metadata

    /// <summary>
    /// Created by Id.
    /// </summary>
    public Guid CreatedById { get; set; }

    /// <summary>
    /// CreatedAt.
    /// </summary>
    public DateTimeOffset CreatedAt { get; set; }

    /// <summary>
    /// Updated at.
    /// </summary>
    public DateTimeOffset UpdatedAt { get; set; }

    #endregion
}