namespace ApiaryManagementSystem.Application.Features.BeeQueens.Commands.CreateBeeQueen;

using System.Threading;
using System.Threading.Tasks;
using ApiaryManagementSystem.Application.Common.Interfaces;
using ApiaryManagementSystem.Domain.Events.BeeQueens;
using ApiaryManagementSystem.Domain.Models.BeeQueens;
using MediatR;

public sealed class CreateBeeQueenCommand : IRequest<Guid>
{
    public int Year { get; init; }

    public string? ColorMark { get; init; }

    public bool IsAlive { get; init; }

    public Guid HiveId { get; init; }
}

internal sealed class CreateBeeQueenCommandHandler(IApplicationDbContext dbContext) : IRequestHandler<CreateBeeQueenCommand, Guid>
{
    public async Task<Guid> Handle(CreateBeeQueenCommand request, CancellationToken cancellationToken)
    {
        var beeQueen = new BeeQueen(
            request.Year,
            request.ColorMark,
            request.IsAlive,
            request.HiveId);

        dbContext.BeeQueens.Add(beeQueen);

        beeQueen.AddDomainEvent(new BeeQueenCreatedEvent());

        await dbContext.SaveChangesAsync(cancellationToken);

        return beeQueen.Id;
    }
}