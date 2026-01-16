using CSharpFunctionalExtensions;
using FluentValidation;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using PetFamily.Core.Abstractions;
using PetFamily.Core.Extensions;
using PetFamily.Discussions.Application.Abstractions;
using PetFamily.Discussions.Domain.Entities;
using PetFamily.SharedKernel.CustomErrors;
using PetFamily.SharedKernel.Structs;
using PetFamily.SharedKernel.ValueObjects.Ids;

namespace PetFamily.Discussions.Application.Commands;

public class CreateDiscussionHandler : ICommandHandler<Guid, CreateDiscussionCommand>
{
    private readonly IDiscussionsRepository _discussionsRepository;
    private readonly ILogger<CreateDiscussionHandler> _logger;
    private readonly IValidator<CreateDiscussionCommand> _validator;
    private readonly IUnitOfWork _unitOfWork;

    public CreateDiscussionHandler(
        IDiscussionsRepository discussionsRepository,
        ILogger<CreateDiscussionHandler> logger,
        IValidator<CreateDiscussionCommand> validator,
        [FromKeyedServices(UnitOfWorkSelector.Discussions)]
        IUnitOfWork unitOfWork)
    {
        _discussionsRepository = discussionsRepository;
        _logger = logger;
        _validator = validator;
        _unitOfWork = unitOfWork;
    }

    public async Task<Result<Guid, ErrorList>> HandleAsync(
        CreateDiscussionCommand command,
        CancellationToken cancellationToken)
    {
        var validationResult = await _validator.ValidateAsync(command, cancellationToken);
        if (validationResult.IsValid == false)
            return validationResult.ToErrorList();

        var relationId = RelationId.Create(command.RelationId);

        var userIds = command.UserIds.Select(UserId.Create).ToList();

        var discussion = Discussion.Create(relationId, userIds);
        
        if (discussion.IsFailure)
            return discussion.Error.ToErrorList();
        
        await _discussionsRepository.AddAsync(discussion.Value, cancellationToken);
        await _unitOfWork.SaveChangesAsync(cancellationToken);

        _logger.LogInformation(
            "Created discussion with Id = {id}", discussion.Value.Id.Value);

        return discussion.Value.Id.Value;
    }
}