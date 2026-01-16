namespace ApiaryManagementSystem.Application.Features.BeeQueens.Commands.DeleteBeeQueen;

using System.Threading;
using System.Threading.Tasks;
using ApiaryManagementSystem.Application.Common.Interfaces;
using ApiaryManagementSystem.Domain.Events.BeeQueens;
using Ardalis.GuardClauses;
using MediatR;
using Microsoft.EntityFrameworkCore;

public sealed class DeleteBeeQueenCommand : IRequest
{
    public Guid BeeQueenId { get; init; }
}

internal sealed class DeleteBeeQueenCommandHandler(IApplicationDbContext dbContext) : IRequestHandler<DeleteBeeQueenCommand>
{
    public async Task Handle(DeleteBeeQueenCommand request, CancellationToken cancellationToken)
    {
        var beeQueen = await dbContext.BeeQueens
            .Where(x => x.Id == request.BeeQueenId)
            .FirstOrDefaultAsync(cancellationToken);

        Guard.Against.NotFound(request.BeeQueenId, beeQueen);

        dbContext.BeeQueens.Remove(beeQueen);

        beeQueen.AddDomainEvent(new BeeQueenDeletedEvent());

        await dbContext.SaveChangesAsync(cancellationToken);
    }
}