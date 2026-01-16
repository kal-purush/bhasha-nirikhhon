using CSharpFunctionalExtensions;
using FluentValidation;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using PetFamily.Core.Abstractions;
using PetFamily.Core.Extensions;
using PetFamily.Discussions.Application.Abstractions;
using PetFamily.Discussions.Domain.ValueObjects;
using PetFamily.SharedKernel.CustomErrors;
using PetFamily.SharedKernel.Structs;
using PetFamily.SharedKernel.ValueObjects.Ids;

namespace PetFamily.Discussions.Application.Commands.EditMessage;
public class EditMessageHandler : ICommandHandler<Guid, EditMessageCommand>
{
    private readonly IDiscussionsRepository _discussionsRepository;
    private readonly ILogger<EditMessageHandler> _logger;
    private readonly IValidator<EditMessageCommand> _validator;
    private readonly IUnitOfWork _unitOfWork;

    public EditMessageHandler(
        IDiscussionsRepository discussionsRepository,
        ILogger<EditMessageHandler> logger,
        IValidator<EditMessageCommand> validator,
        [FromKeyedServices(UnitOfWorkSelector.Discussions)]
        IUnitOfWork unitOfWork)
    {
        _discussionsRepository = discussionsRepository;
        _logger = logger;
        _validator = validator;
        _unitOfWork = unitOfWork;
    }

    public async Task<Result<Guid, ErrorList>> HandleAsync(
        EditMessageCommand command,
        CancellationToken cancellationToken)
    {
        var validationResult = await _validator.ValidateAsync(command, cancellationToken);
        if (validationResult.IsValid == false)
            return validationResult.ToErrorList();

        var relationId = RelationId.Create(command.RelationId);
        
        var messageId = MessageId.Create(command.MessageId);

        var userId = UserId.Create(command.UserId);

        var newMessageText = MessageText.Create(command.NewMessageText).Value;
        
        var discussion = await _discussionsRepository.GetByRelationIdAsync(relationId, cancellationToken);
        if (discussion.IsFailure)
            return Errors.General.ValueNotFound(relationId).ToErrorList();
        
        var result = discussion.Value.EditMessage(userId, messageId, newMessageText);
        
        if (result.IsFailure)
            return result.Error.ToErrorList();
        
        await _unitOfWork.SaveChangesAsync(cancellationToken);

        _logger.LogInformation(
            "User with Id = {ID1} edited message with Id = {ID2} in discussion with relationId = {ID3}",
            userId.Value,
            messageId.Value,
            discussion.Value.RelationId.Value);

        return discussion.Value.Id.Value;
    }
}