namespace ApiaryManagementSystem.Application.Features.BeeQueens.Commands.UpdateBeeQueen;

using System.Threading;
using System.Threading.Tasks;
using ApiaryManagementSystem.Application.Common.Interfaces;
using ApiaryManagementSystem.Domain.Events.BeeQueens;
using Ardalis.GuardClauses;
using MediatR;

public sealed class UpdateBeeQueenCommand : IRequest
{
    public Guid Id { get; init; }

    public int Year { get; init; }

    public string? ColorMark { get; init; }

    public bool IsAlive { get; init; }

    public Guid HiveId { get; init; }
}

internal sealed class UpdateBeeQueenCommandHandler(IApplicationDbContext dbContext) : IRequestHandler<UpdateBeeQueenCommand>
{
    public async Task Handle(UpdateBeeQueenCommand request, CancellationToken cancellationToken)
    {
        var beeQueen = await dbContext.BeeQueens
            .FindAsync([request.Id], cancellationToken);

        Guard.Against.NotFound(request.Id, beeQueen);

        beeQueen.UpdateBeeQueen(
            request.Year,
            request.ColorMark,
            request.IsAlive,
            request.HiveId);

        beeQueen.AddDomainEvent(new BeeQueenUpdatedEvent());

        await dbContext.SaveChangesAsync(cancellationToken);
    }
}