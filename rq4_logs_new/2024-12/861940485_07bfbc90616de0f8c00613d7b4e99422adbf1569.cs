namespace ApiaryManagementSystem.Application.Features.Harvests.Commands.DeleteHarvest;

using System.Threading;
using System.Threading.Tasks;
using ApiaryManagementSystem.Application.Common.Interfaces;
using ApiaryManagementSystem.Domain.Events.Harvests;
using Ardalis.GuardClauses;
using MediatR;
using Microsoft.EntityFrameworkCore;

public sealed class DeleteHarvestCommand : IRequest
{
    public Guid HarvestId { get; init; }
}

internal sealed class DeleteHarvestCommandHandler(IApplicationDbContext dbContext) : IRequestHandler<DeleteHarvestCommand>
{
    public async Task Handle(DeleteHarvestCommand request, CancellationToken cancellationToken)
    {
        var harvest = await dbContext.Harvests
            .Where(x => x.Id == request.HarvestId)
            .FirstOrDefaultAsync(cancellationToken);

        Guard.Against.NotFound(request.HarvestId, harvest);

        dbContext.Harvests.Remove(harvest);

        harvest.AddDomainEvent(new HarvestsDeletedEvent());

        await dbContext.SaveChangesAsync(cancellationToken);
    }
}