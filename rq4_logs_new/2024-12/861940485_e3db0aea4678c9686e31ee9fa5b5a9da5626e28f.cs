namespace ApiaryManagementSystem.Domain.Models.Harvests;

using ApiaryManagementSystem.Domain.Common;
using ApiaryManagementSystem.Domain.Models.Hives;

public sealed class Harvest(
    DateTime date,
    decimal amount,
    string product,
    Guid hiveId) : BaseAuditableEntity
{
    public DateTime Date { get; init; } = date;

    public decimal Amount { get; init; } = amount;

    public string Product { get; init; } = product; //(e.g., Honey, Beeswax, Pollen, Propolis)

    public Guid HiveId { get; init; } = hiveId;

    public Hive Hive { get; init; } = null!;    // Required reference navigation to principal
}