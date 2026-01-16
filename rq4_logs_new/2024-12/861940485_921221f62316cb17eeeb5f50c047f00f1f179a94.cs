namespace ApiaryManagementSystem.Application.Features.BeeQueens.Queries.GetBeeQueenById;

using System.Threading;
using System.Threading.Tasks;
using ApiaryManagementSystem.Application.Common.Interfaces;
using Ardalis.GuardClauses;
using AutoMapper;
using AutoMapper.QueryableExtensions;
using MediatR;
using Microsoft.EntityFrameworkCore;

public sealed class GetBeeQueenByIdCommand : IRequest<BeeQueenModel>
{
    public Guid BeeQueenId { get; init; }
}

internal sealed class GetBeeQueenByIdCommandHandler(
    IApplicationDbContext dbContext,
    IMapper mapper) : IRequestHandler<GetBeeQueenByIdCommand, BeeQueenModel>
{
    public async Task<BeeQueenModel> Handle(GetBeeQueenByIdCommand request, CancellationToken cancellationToken)
    {
        var beeQueen = await dbContext.BeeQueens
            .Where(x => x.Id == request.BeeQueenId)
            .AsNoTracking()
            .ProjectTo<BeeQueenModel>(mapper.ConfigurationProvider)
            .FirstOrDefaultAsync(cancellationToken);

        Guard.Against.NotFound(request.BeeQueenId, beeQueen);

        return beeQueen;
    }
}