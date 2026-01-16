using Codend.Domain.Core.Errors;
using Codend.Domain.Core.Extensions;
using Codend.Domain.Core.Primitives;
using FluentResults;

namespace Codend.Domain.ValueObjects;

/// <summary>
/// Project sprint goal.
/// </summary>
public sealed class SprintGoal : ValueObject
{
    /// <summary>
    /// Maximum goal length.
    /// </summary>
    public const int MaxLength = 200;
    
    /// <summary>
    /// Sprint goal value
    /// </summary>
    public string Goal { get; }
    
    public SprintGoal(string goal)
    {
        Goal = goal;
    }

    /// <summary>
    /// Creates <see cref="SprintGoal"/> instance.
    /// </summary>
    /// <param name="goal">Goal value</param>
    /// <returns>The result of sprint goal creation. Returns sprint goal or an error.</returns>
    public static Result<SprintGoal> Create(string goal)
    {
        return Result
            .Ok(new SprintGoal(goal))
            .Ensure(() => !string.IsNullOrEmpty(goal), new DomainErrors.SprintGoal.NullOrEmpty())
            .Ensure(() => goal.Length < MaxLength, new DomainErrors.SprintGoal.GoalTooLong());
    }

    protected override IEnumerable<object> GetAtomicValues()
    {
        yield return Goal;
    }
}