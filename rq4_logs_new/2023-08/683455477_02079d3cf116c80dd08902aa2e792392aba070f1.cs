using Codend.Domain.Core.Errors;
using Codend.Domain.Core.Extensions;
using Codend.Domain.Core.Primitives;
using FluentResults;

namespace Codend.Domain.ValueObjects;

/// <summary>
/// Project version tag.
/// </summary>
public sealed class ProjectVersionTag : ValueObject
{
    /// <summary>
    /// Maximum description length.
    /// </summary>
    public const int MaxLength = 20;

    /// <summary>
    /// Version tag value.
    /// </summary>
    public string Tag { get; }

    private ProjectVersionTag(string tag)
    {
        Tag = tag;
    }

    /// <summary>
    /// Creates <see cref="ProjectVersionTag"/> instance.
    /// </summary>
    /// <param name="tag">Tag value.</param>
    /// <returns>The result of project version tag creation. Returns project version tag or an error.</returns>
    public static Result<ProjectVersionTag> Create(string tag)
    {
        return Result
            .Ok(new ProjectVersionTag(tag))
            .Ensure(() => string.IsNullOrEmpty(tag), new DomainErrors.ProjectVersionTag.NullOrEmpty())
            .Ensure(() => tag.Length < MaxLength, new DomainErrors.ProjectVersionTag.TagTooLong());
    }

    /// <inheritdoc />
    protected override IEnumerable<object> GetAtomicValues()
    {
        yield return Tag;
    }
}