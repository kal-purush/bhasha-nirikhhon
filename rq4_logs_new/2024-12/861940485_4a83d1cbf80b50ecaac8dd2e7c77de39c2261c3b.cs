namespace ApiaryManagementSystem.Application.Features.Harvests.Commands.CreateHarvest;

using System.Threading;
using System.Threading.Tasks;
using ApiaryManagementSystem.Application.Common.Interfaces;
using ApiaryManagementSystem.Domain.Events.Harvests;
using ApiaryManagementSystem.Domain.Models.Harvests;
using MediatR;

public sealed class CreateHarvestCommand : IRequest<Guid>
{
    public DateTime Date { get; init; }

    public decimal Amount { get; init; }

    public required string Product { get; init; }

    public Guid HiveId { get; init; }
}

internal sealed class CreateHarvestCommandHandler(IApplicationDbContext dbContext) : IRequestHandler<CreateHarvestCommand, Guid>
{
    public async Task<Guid> Handle(CreateHarvestCommand request, CancellationToken cancellationToken)
    {
        var harvest = new Harvest(
            request.Date,
            request.Amount,
            request.Product,
            request.HiveId);

        dbContext.Harvests.Add(harvest);

        harvest.AddDomainEvent(new HarvestsCreatedEvent());

        await dbContext.SaveChangesAsync(cancellationToken);

        return harvest.Id;
    }
}