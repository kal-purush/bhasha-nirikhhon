using Codend.Domain.Core.Errors;
using Codend.Domain.Core.Extensions;
using Codend.Domain.Core.Primitives;
using FluentResults;

namespace Codend.Domain.ValueObjects;

/// <summary>
/// Project version name.
/// </summary>
public sealed class ProjectVersionName : ValueObject
{
    /// <summary>
    /// Maximum name length.
    /// </summary>
    public const int MaxLength = 50;

    /// <summary>
    /// Version name value.
    /// </summary>
    public string Name { get; }

    private ProjectVersionName(string name)
    {
        Name = name;
    }

    public static Result<ProjectVersionName> Create(string name)
    {
        return Result
            .Ok(new ProjectVersionName(name))
            .Ensure(() => string.IsNullOrEmpty(name), new DomainErrors.ProjectVersionName.NullOrEmpty())
            .Ensure(() => name.Length < MaxLength, new DomainErrors.ProjectVersionName.NameTooLong());
    }

    /// <inheritdoc />
    protected override IEnumerable<object> GetAtomicValues()
    {
        yield return Name;
    }
}