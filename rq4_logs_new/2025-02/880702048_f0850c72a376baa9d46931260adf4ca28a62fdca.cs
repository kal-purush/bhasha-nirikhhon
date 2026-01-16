using CSharpFunctionalExtensions;
using FluentValidation;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using P2Project.Core;
using P2Project.Core.Extensions;
using P2Project.Core.Interfaces;
using P2Project.Core.Interfaces.Commands;
using P2Project.Discussions.Agreements;
using P2Project.Discussions.Application.Interfaces;
using P2Project.Discussions.Domain;
using P2Project.SharedKernel.Errors;

namespace P2Project.Discussions.Application.DiscussionsManagement.Commands.Close;

public class CloseHandler :
    ICommandHandler<Guid, CloseCommand>
{
    private readonly IValidator<CloseCommand> _validator;
    private readonly IDiscussionsRepository _discussionRepository;
    private readonly IDiscussionsAgreement _discussionsAgreement;
    private readonly IUnitOfWork _unitOfWork;
    private readonly ILogger<CloseHandler> _logger;

    public CloseHandler(
        IValidator<CloseCommand> validator,
        IDiscussionsRepository discussionRepository,
        IDiscussionsAgreement discussionsAgreement,
        [FromKeyedServices(Modules.Discussions)] IUnitOfWork unitOfWork,
        ILogger<CloseHandler> logger)
    {
        _validator = validator;
        _discussionRepository = discussionRepository;
        _discussionsAgreement = discussionsAgreement;
        _unitOfWork = unitOfWork;
        _logger = logger;
    }

    public async Task<Result<Guid, ErrorList>> Handle(
        CloseCommand command,
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
        
        if(discussionExist.Value.Status == DiscussionStatus.Closed)
            return Errors.General.Failure("already.closed").ToErrorList();
        
        var messageId = await _discussionsAgreement.CreateMessage(
            command.UserId, discussionExist.Value.DiscussionUsers.ReviewingUserId,
            command.Comment,
            cancellationToken);
        if(messageId.IsFailure)
            return Errors.General.Failure("message").ToErrorList();
        
        discussionExist.Value.Close();
        
        await _unitOfWork.SaveChanges(cancellationToken);
        
        _logger.LogInformation(
            "Discussion with {discussionId} closed by {userId}",
            discussionExist.Value.DiscussionId,
            command.UserId);

        return discussionExist.Value.DiscussionId;
    }
}