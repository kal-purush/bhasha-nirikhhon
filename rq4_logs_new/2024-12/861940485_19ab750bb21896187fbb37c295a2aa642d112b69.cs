namespace ApiaryManagementSystem.Domain.Models.Diseases;

using ApiaryManagementSystem.Domain.Common;
using ApiaryManagementSystem.Domain.Models.Hives;

public sealed class Disease(
    string name,
    string treatment,
    string? description,
    Guid hiveId) : BaseAuditableEntity
{
    public string Name { get; private set; } = name;

    public string Treatment { get; private set; } = treatment;

    public string? Description { get; private set; } = description;

    public Guid HiveId { get; private set; } = hiveId;

    public Hive Hive { get; init; } = null!;    // Required reference navigation to principal
}