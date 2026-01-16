using CSharpFunctionalExtensions;
using FluentValidation;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using PetFamily.Core.Abstractions;
using PetFamily.Core.Extensions;
using PetFamily.Discussions.Application.Abstractions;
using PetFamily.Discussions.Domain.Entities;
using PetFamily.Discussions.Domain.ValueObjects;
using PetFamily.SharedKernel.CustomErrors;
using PetFamily.SharedKernel.Structs;
using PetFamily.SharedKernel.ValueObjects.Ids;

namespace PetFamily.Discussions.Application.Commands.AddMessage;
public class AddMessageHandler : ICommandHandler<Guid, AddMessageCommand>
{
    private readonly IDiscussionsRepository _discussionsRepository;
    private readonly ILogger<AddMessageHandler> _logger;
    private readonly IValidator<AddMessageCommand> _validator;
    private readonly IUnitOfWork _unitOfWork;

    public AddMessageHandler(
        IDiscussionsRepository discussionsRepository,
        ILogger<AddMessageHandler> logger,
        IValidator<AddMessageCommand> validator,
        [FromKeyedServices(UnitOfWorkSelector.Discussions)]
        IUnitOfWork unitOfWork)
    {
        _discussionsRepository = discussionsRepository;
        _logger = logger;
        _validator = validator;
        _unitOfWork = unitOfWork;
    }

    public async Task<Result<Guid, ErrorList>> HandleAsync(
        AddMessageCommand command,
        CancellationToken cancellationToken)
    {
        var validationResult = await _validator.ValidateAsync(command, cancellationToken);
        if (validationResult.IsValid == false)
            return validationResult.ToErrorList();

        var relationId = RelationId.Create(command.RelationId);

        var userId = UserId.Create(command.UserId);

        var messageText = MessageText.Create(command.MessageText).Value;
        
        var message = new Message(messageText, userId);

        var discussion = await _discussionsRepository.GetByRelationIdAsync(relationId, cancellationToken);
        if (discussion.IsFailure)
            return Errors.General.ValueNotFound(relationId).ToErrorList();
        
        var result = discussion.Value.AddMessage(message);
        
        if (result.IsFailure)
            return result.Error.ToErrorList();
        
        await _unitOfWork.SaveChangesAsync(cancellationToken);

        _logger.LogInformation(
            "User with Id = {ID1} added message to discussion with relationId = {ID2}",
            userId.Value,
            discussion.Value.RelationId.Value);

        return discussion.Value.Id.Value;
    }
}