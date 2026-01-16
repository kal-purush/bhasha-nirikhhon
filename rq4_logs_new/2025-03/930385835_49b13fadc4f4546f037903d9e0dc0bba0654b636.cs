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

namespace PetFamily.Discussions.Application.Commands.RemoveMessage;

public class RemoveMessageHandler : ICommandHandler<Guid, RemoveMessageCommand>
{
    private readonly IDiscussionsRepository _discussionsRepository;
    private readonly ILogger<RemoveMessageHandler> _logger;
    private readonly IValidator<RemoveMessageCommand> _validator;
    private readonly IUnitOfWork _unitOfWork;

    public RemoveMessageHandler(
        IDiscussionsRepository discussionsRepository,
        ILogger<RemoveMessageHandler> logger,
        IValidator<RemoveMessageCommand> validator,
        [FromKeyedServices(UnitOfWorkSelector.Discussions)]
        IUnitOfWork unitOfWork)
    {
        _discussionsRepository = discussionsRepository;
        _logger = logger;
        _validator = validator;
        _unitOfWork = unitOfWork;
    }

    public async Task<Result<Guid, ErrorList>> HandleAsync(
        RemoveMessageCommand command,
        CancellationToken cancellationToken)
    {
        var validationResult = await _validator.ValidateAsync(command, cancellationToken);
        if (validationResult.IsValid == false)
            return validationResult.ToErrorList();

        var relationId = RelationId.Create(command.RelationId);
        
        var messageId = MessageId.Create(command.MessageId);

        var userId = UserId.Create(command.UserId);
        
        var discussion = await _discussionsRepository.GetByRelationIdAsync(relationId, cancellationToken);
        if (discussion.IsFailure)
            return Errors.General.ValueNotFound(relationId).ToErrorList();
        
        var result = discussion.Value.RemoveMessage(userId, messageId);
        
        if (result.IsFailure)
            return result.Error.ToErrorList();
        
        await _unitOfWork.SaveChangesAsync(cancellationToken);

        _logger.LogInformation(
            "User with Id = {ID1} removed message with Id = {ID2} from discussion with relationId = {ID3}",
            userId.Value,
            messageId.Value,
            discussion.Value.RelationId.Value);

        return discussion.Value.Id.Value;
    }
}