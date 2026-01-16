using CSharpFunctionalExtensions;
using FluentValidation;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using P2Project.Core;
using P2Project.Core.Extensions;
using P2Project.Core.Interfaces;
using P2Project.Core.Interfaces.Commands;
using P2Project.Discussions.Application.Interfaces;
using P2Project.SharedKernel.Errors;
using P2Project.SharedKernel.ValueObjects;

namespace P2Project.Discussions.Application.DiscussionsManagement.Commands.EditMessage;

public class EditMessageHandler :
    ICommandHandler<Guid, EditMessageCommand>
{
    private readonly IValidator<EditMessageCommand> _validator;
    private readonly IDiscussionsRepository _discussionRepository;
    private readonly IUnitOfWork _unitOfWork;
    private readonly ILogger<EditMessageHandler> _logger;

    public EditMessageHandler(
        IValidator<EditMessageCommand> validator,
        IDiscussionsRepository discussionRepository,
        [FromKeyedServices(Modules.Discussions)] IUnitOfWork unitOfWork,
        ILogger<EditMessageHandler> logger)
    {
        _validator = validator;
        _discussionRepository = discussionRepository;
        _unitOfWork = unitOfWork;
        _logger = logger;
    }

    public async Task<Result<Guid, ErrorList>> Handle(
        EditMessageCommand command,
        CancellationToken cancellationToken = default)
    {
        var validationResult = await _validator.ValidateAsync(
            command, cancellationToken);
        if (validationResult.IsValid == false)
            return validationResult.ToErrorList();
        
        var discussionExist = await _discussionRepository.GetById(
            command.DiscussionId, cancellationToken);
        if (discussionExist.IsFailure)
            return Errors.General.NotFound(command.DiscussionId).ToErrorList();

        var messageText = Content.Create(command.MessageText).Value;
        
        discussionExist.Value.EditMessage(command.MessageId, command.SenderId, messageText);

        await _unitOfWork.SaveChanges(cancellationToken);
        
        _logger.LogInformation(
            "Message {messageId} was edited in discussion with {discussionId}",
            command.MessageId,
            discussionExist.Value.DiscussionId);
        
        return command.MessageId;
    }
}