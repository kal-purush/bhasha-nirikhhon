namespace ApiaryManagementSystem.Domain.Models.BeeQueens;

using ApiaryManagementSystem.Domain.Common;
using ApiaryManagementSystem.Domain.Models.Hives;

public sealed class BeeQueen(
    int year,
    string? colorMark,
    bool isAlive,
    Guid hiveId) : BaseAuditableEntity
{
    public int Year { get; init; } = year;

    public string? ColorMark { get; init; } = colorMark;

    public bool IsAlive { get; init; } = isAlive;

    public Guid HiveId { get; private set; } = hiveId;

    public Hive Hive { get; init; } = null!;    // Required reference navigation to principal
}