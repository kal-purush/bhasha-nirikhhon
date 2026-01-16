using CSharpFunctionalExtensions;
using FluentValidation;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using P2Project.Core;
using P2Project.Core.Extensions;
using P2Project.Core.Interfaces;
using P2Project.Core.Interfaces.Commands;
using P2Project.Discussions.Application.Interfaces;
using P2Project.Discussions.Domain.Entities;
using P2Project.SharedKernel.Errors;
using P2Project.SharedKernel.ValueObjects;

namespace P2Project.Discussions.Application.DiscussionsManagement.Commands.CreateMessage;

public class CreateMessageHandler :
    ICommandHandler<Message, CreateMessageCommand>
{
    private readonly IValidator<CreateMessageCommand> _validator;
    private readonly IDiscussionsRepository _discussionsRepository;
    private readonly ILogger<CreateMessageCommand> _logger;
    private readonly IUnitOfWork _unitOfWork;

    public CreateMessageHandler(
        IValidator<CreateMessageCommand> validator,
        IDiscussionsRepository discussionsRepository,
        ILogger<CreateMessageCommand> logger,
        [FromKeyedServices(Modules.Discussions)] IUnitOfWork unitOfWork)
    {
        _validator = validator;
        _discussionsRepository = discussionsRepository;
        _logger = logger;
        _unitOfWork = unitOfWork;
    }

    public async Task<Result<Message, ErrorList>> Handle(
        CreateMessageCommand command,
        CancellationToken cancellationToken = default)
    {
        var validationResult = await _validator.ValidateAsync(
            command, cancellationToken);
        if (validationResult.IsValid == false)
            return validationResult.ToErrorList();
        
        var discussionExist = await _discussionsRepository.GetByParticipantId(
            command.SenderId, cancellationToken);
        if (discussionExist.IsFailure)
            return discussionExist.Error.ToErrorList();
        
        var newMessage = Message.Create(
            discussionExist.Value.DiscussionId,
            command.SenderId,
            Content.Create(command.Message).Value);

        discussionExist.Value.AddMessage(newMessage);

        await _unitOfWork.SaveChanges(cancellationToken);
        
        _logger.LogInformation(
            "Message was created in discussion with id {discussionId}",
            discussionExist.Value.DiscussionId);
        
        return newMessage;
    }
}