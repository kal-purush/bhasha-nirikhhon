using CSharpFunctionalExtensions;
using FluentValidation;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using P2Project.Accounts.Agreements;
using P2Project.Core;
using P2Project.Core.Extensions;
using P2Project.Core.Interfaces;
using P2Project.Core.Interfaces.Commands;
using P2Project.Discussions.Agreements;
using P2Project.SharedKernel.Errors;
using P2Project.VolunteerRequests.Application.Interfaces;

namespace P2Project.VolunteerRequests.Application.VolunteerRequestsManagement.Commands.SetReopenStatus;

public class SetReopenStatusHandler :
    ICommandHandler<Guid, SetReopenStatusCommand>
{
    private readonly IValidator<SetReopenStatusCommand> _validator;
    private readonly IAccountsAgreements _accountsAgreements;
    private readonly IDiscussionsAgreement _discussionsAgreement;
    private readonly IVolunteerRequestsRepository _volunteerRequestsRepository;
    private readonly IUnitOfWork _unitOfWork;
    private readonly ILogger<SetReopenStatusHandler> _logger;

    public SetReopenStatusHandler(
        IValidator<SetReopenStatusCommand> validator,
        IAccountsAgreements accountsAgreements,
        IDiscussionsAgreement discussionsAgreement,
        IVolunteerRequestsRepository volunteerRequestsRepository,
        [FromKeyedServices(Modules.VolunteerRequests)] IUnitOfWork unitOfWork,
        ILogger<SetReopenStatusHandler> logger)
    {
        _validator = validator;
        _accountsAgreements = accountsAgreements;
        _discussionsAgreement = discussionsAgreement;
        _volunteerRequestsRepository = volunteerRequestsRepository;
        _unitOfWork = unitOfWork;
        _logger = logger;
    }

    public async Task<Result<Guid, ErrorList>> Handle(
        SetReopenStatusCommand command,
        CancellationToken cancellationToken = default)
    {
        var validationResult = await _validator.ValidateAsync(
            command, cancellationToken);
        if (validationResult.IsValid == false)
            return validationResult.ToErrorList();
        
        var isUserBanned = await _accountsAgreements.IsUserBannedForVolunteerRequests(command.UserId, cancellationToken);
        if (isUserBanned)
            return Errors.General.Failure("user is banned").ToErrorList();
        
        var existedRequest = await _volunteerRequestsRepository.GetById(
            command.RequestId, cancellationToken);
        if (existedRequest.IsFailure)
            return Errors.General.NotFound(command.RequestId).ToErrorList();
        
        existedRequest.Value.Refresh();
        
        var messageId = await _discussionsAgreement.CreateMessage(
            command.UserId, existedRequest.Value.UserId,
            command.Comment,
            cancellationToken);
        if(messageId.IsFailure)
            return Errors.General.Failure("message").ToErrorList();
        
        await _unitOfWork.SaveChanges(cancellationToken);
        
        _logger.LogInformation(
            "Volunteer request with id {requestId} was reopened with submitted status",
            command.RequestId);

        return existedRequest.Value.RequestId;
    }
}