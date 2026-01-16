namespace ApiaryManagementSystem.Application.Features.Harvests.Commands.UpdateHarvest;

using System.Threading;
using System.Threading.Tasks;
using ApiaryManagementSystem.Application.Common.Interfaces;
using ApiaryManagementSystem.Domain.Events.Harvests;
using Ardalis.GuardClauses;
using MediatR;

public sealed class UpdateHarvestCommand : IRequest
{
    public Guid Id { get; set; }

    public DateTime Date { get; init; }

    public decimal Amount { get; init; }

    public required string Product { get; init; }

    public Guid HiveId { get; init; }
}

internal sealed class UpdateHarvestCommandHandler(IApplicationDbContext dbContext) : IRequestHandler<UpdateHarvestCommand>
{
    public async Task Handle(UpdateHarvestCommand request, CancellationToken cancellationToken)
    {
        var harvest = await dbContext.Harvests
            .FindAsync([request.Id], cancellationToken);

        Guard.Against.NotFound(request.Id, harvest);

        harvest.UpdateHarvest(
            request.Date,
            request.Amount,
            request.Product,
            request.HiveId);

        harvest.AddDomainEvent(new HarvestsUpdatedEvent());

        await dbContext.SaveChangesAsync(cancellationToken);
    }
}