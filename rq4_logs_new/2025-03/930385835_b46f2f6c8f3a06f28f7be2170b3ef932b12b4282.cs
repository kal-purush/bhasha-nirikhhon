using CSharpFunctionalExtensions;
using FluentValidation;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using PetFamily.Core.Abstractions;
using PetFamily.Core.Extensions;
using PetFamily.Discussions.Application.Abstractions;
using PetFamily.SharedKernel.CustomErrors;
using PetFamily.SharedKernel.Structs;
using PetFamily.SharedKernel.ValueObjects.Ids;

namespace PetFamily.Discussions.Application.Commands.CloseDiscussion;

public class CloseDiscussionHandler : ICommandHandler<Guid, CloseDiscussionCommand>
{
    private readonly IDiscussionsRepository _discussionsRepository;
    private readonly ILogger<CloseDiscussionHandler> _logger;
    private readonly IValidator<CloseDiscussionCommand> _validator;
    private readonly IUnitOfWork _unitOfWork;

    public CloseDiscussionHandler(
        IDiscussionsRepository discussionsRepository,
        ILogger<CloseDiscussionHandler> logger,
        IValidator<CloseDiscussionCommand> validator,
        [FromKeyedServices(UnitOfWorkSelector.Discussions)]
        IUnitOfWork unitOfWork)
    {
        _discussionsRepository = discussionsRepository;
        _logger = logger;
        _validator = validator;
        _unitOfWork = unitOfWork;
    }

    public async Task<Result<Guid, ErrorList>> HandleAsync(
        CloseDiscussionCommand command,
        CancellationToken cancellationToken)
    {
        var validationResult = await _validator.ValidateAsync(command, cancellationToken);
        if (validationResult.IsValid == false)
            return validationResult.ToErrorList();

        var relationId = RelationId.Create(command.RelationId);
       
        var discussion = await _discussionsRepository
            .GetByRelationIdAsync(relationId, cancellationToken);

        if (discussion.IsFailure)
            return discussion.Error.ToErrorList();

        discussion.Value.Close();
        await _unitOfWork.SaveChangesAsync(cancellationToken);
        
        _logger.LogInformation($"Discussion with Id = {discussion.Value.Id} has been closed.");

        return discussion.Value.Id.Value;
    }
}