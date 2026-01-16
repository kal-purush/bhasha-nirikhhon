using CSharpFunctionalExtensions;
using Microsoft.Extensions.DependencyInjection;
using PetFamily.Core.Abstractions;
using PetFamily.Discussions.Application.Abstractions;
using PetFamily.Discussions.Contracts;
using PetFamily.Discussions.Contracts.Requests;
using PetFamily.SharedKernel.CustomErrors;
using PetFamily.SharedKernel.Structs;

namespace PetFamily.Discussions.Presentation.Contracts;

public class CloseDiscussionContract : ICloseDiscussionContract
{
    private readonly IDiscussionsRepository _discussionsRepository;
    private readonly IUnitOfWork _unitOfWork;

    public CloseDiscussionContract(
        IDiscussionsRepository discussionsRepository,
        [FromKeyedServices(UnitOfWorkSelector.Discussions)]
        IUnitOfWork unitOfWork)
    {
        _discussionsRepository = discussionsRepository;
        _unitOfWork = unitOfWork;
    }

    public async Task<Result<Guid, Error>> CloseDiscussion(
        CloseDiscussionRequest request,
        CancellationToken cancellationToken)
    {
        var discussion = await _discussionsRepository
            .GetByRelationIdAsync(request.RelationId, cancellationToken);

        if (discussion.IsFailure)
            return discussion.Error;

        discussion.Value.Close();
        await _unitOfWork.SaveChangesAsync(cancellationToken);

        return discussion.Value.Id.Value;
    }
}