using Codend.Domain.Core.Errors;
using Codend.Domain.Core.Extensions;
using Codend.Domain.Core.Primitives;
using FluentResults;

namespace Codend.Domain.ValueObjects;

public class ProjectDescription : ValueObject
{
    /// <summary>
    /// Maximum description length.
    /// </summary>
    public const int MaxLength = 2000;

    /// <summary>
    /// Project description value.
    /// </summary>
    public string Description { get; }

    private ProjectDescription(string description)
    {
        Description = description;
    }

    /// <summary>
    /// Creates <see cref="ProjectDescription"/> instance.
    /// </summary>
    /// <param name="name">Description value.</param>
    /// <returns>The result of creation. Returns project description or an error.</returns>
    public static Result<ProjectDescription> Create(string name)
    {
        return Result
            .Ok(new ProjectDescription(name))
            .Ensure(() => name.Length > MaxLength, new DomainErrors.ProjectDescription.DescriptionTooLong());
    }

    /// <inheritdoc />
    protected override IEnumerable<object> GetAtomicValues()
    {
        yield return Description;
    }
}