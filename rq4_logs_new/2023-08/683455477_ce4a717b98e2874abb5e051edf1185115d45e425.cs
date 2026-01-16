using Codend.Domain.Core.Errors;
using Codend.Domain.Core.Extensions;
using Codend.Domain.Core.Primitives;
using FluentResults;

namespace Codend.Domain.ValueObjects;

/// <summary>
/// Project version changelog.
/// </summary>
public sealed class ProjectVersionChangelog : ValueObject
{
    /// <summary>
    /// Maximum description length.
    /// </summary>
    public const int MaxLength = 3000;

    /// <summary>
    /// Version changelog value.
    /// </summary>
    public string Changelog { get; }

    private ProjectVersionChangelog(string changelog)
    {
        Changelog = changelog;
    }

    /// <summary>
    /// Creates <see cref="ProjectVersionChangelog" /> instance.
    /// </summary>
    /// <param name="changelog">Changelog value.</param>
    /// <returns>The result of project version changelog creation. Returns project version changelog or an error.</returns>
    public static Result<ProjectVersionChangelog> Create(string changelog)
    {
        return Result
            .Ok(new ProjectVersionChangelog(changelog))
            .Ensure(() => changelog.Length < MaxLength, new DomainErrors.ProjectVersionChangelog.DescriptionTooLong());
    }

    /// <inheritdoc />
    protected override IEnumerable<object> GetAtomicValues()
    {
        yield return Changelog;
    }
}