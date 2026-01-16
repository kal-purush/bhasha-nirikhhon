using Codend.Domain.Core.Errors;
using Codend.Domain.Core.Extensions;
using Codend.Domain.Core.Primitives;
using FluentResults;

namespace Codend.Domain.ValueObjects;

/// <summary>
/// ProjectTask description.
/// </summary>
public sealed class ProjectTaskDescription : ValueObject
{
    /// <summary>
    /// Maximum description lenghth.
    /// </summary>
    public const int MaxLength = 2000;

    /// <summary>
    /// Description value.
    /// </summary>
    private string Description { get; }

    private ProjectTaskDescription(string description)
    {
        Description = description;
    }

    /// <summary>
    /// Creates new ProjectTaskDescription value object with given <paramref name="description"/> string.
    /// Additionaly it checkes whether the maximum length is not exceeded and string value is not null/empty.
    /// </summary>
    /// <param name="description">Description for the new ProjectTaskDescription.</param>
    /// <returns>Result of the create operation. Returns ProjetTaskDescription or an error.</returns>
    public static Result<ProjectTaskDescription> Create(string description)
    {
        return Result.Ok(new ProjectTaskDescription(description))
            .Ensure(() => description.Length < MaxLength, new DomainErrors.ProjectTaskDescription.DescriptionTooLong());
    }

    protected override IEnumerable<object> GetAtomicValues()
    {
        yield return Description;
    }
}