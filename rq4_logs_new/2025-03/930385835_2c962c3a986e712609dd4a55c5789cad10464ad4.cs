using CSharpFunctionalExtensions;
using Microsoft.EntityFrameworkCore;
using PetFamily.Core.Abstractions;
using PetFamily.Core.Dto.Discussion;
using PetFamily.Discussions.Application.Abstractions;
using PetFamily.SharedKernel.CustomErrors;

namespace PetFamily.Discussions.Application.Queries.GetDiscussion;

public class GetDiscussionHandler : IQueryHandler<Result<DiscussionDto, ErrorList>, GetDiscussionQuery>
{
    private readonly IReadDbContext _readDbContext;

    public GetDiscussionHandler(IReadDbContext readDbContext)
    {
        _readDbContext = readDbContext;
    }

    public async Task<Result<DiscussionDto, ErrorList>> HandleAsync(
        GetDiscussionQuery query,
        CancellationToken cancellationToken)
    {
        var result = await _readDbContext.Discussions
            .Include(d => d.Messages)
            .FirstOrDefaultAsync(d => d.RelationId == query.RelationId, cancellationToken);

        if (result == null)
            return Errors.General.ValueNotFound(query.RelationId).ToErrorList();

        result.Messages = result.Messages.OrderBy(m => m.CreatedAt).ToList();
        return result;
    }
}