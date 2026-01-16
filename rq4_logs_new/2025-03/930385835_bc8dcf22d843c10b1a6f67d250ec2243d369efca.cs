using CSharpFunctionalExtensions;
using Microsoft.AspNetCore.Identity;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using PetFamily.Accounts.Application.Abstractions;
using PetFamily.Accounts.Contracts;
using PetFamily.Accounts.Contracts.Requests;
using PetFamily.Accounts.Domain.DataModels;
using PetFamily.Core.Abstractions;
using PetFamily.SharedKernel.Constants;
using PetFamily.SharedKernel.CustomErrors;
using PetFamily.SharedKernel.Structs;

namespace PetFamily.Accounts.Presentation.Contracts;

public class CreateVolunteerAccountContract : ICreateVolunteerAccountContract
{
    private readonly UserManager<User> _userManager;
    private readonly IAccountManager _accountManager;
    private readonly RoleManager<Role> _roleManager;
    private readonly IUnitOfWork _unitOfWork;
    private readonly ILogger<CreateVolunteerAccountContract> _logger;

    public CreateVolunteerAccountContract(
        UserManager<User> userManager,
        IAccountManager accountManager,
        RoleManager<Role> roleManager,
        [FromKeyedServices(UnitOfWorkSelector.Accounts)]
        IUnitOfWork unitOfWork,
        ILogger<CreateVolunteerAccountContract> logger)
    {
        _userManager = userManager;
        _accountManager = accountManager;
        _roleManager = roleManager;
        _unitOfWork = unitOfWork;
        _logger = logger;
    }

    public async Task<Result<Guid, Error>> CreateVolunteerAccountAsync(
        CreateVolunteerAccountRequest request,
        CancellationToken cancellationToken)
    {
        var user = await _userManager.Users.FirstOrDefaultAsync(u => u.Id == request.UserId, cancellationToken);
        if (user is null)
            return Errors.General.ValueNotFound(request.UserId);

        var role = await _roleManager.FindByNameAsync(DomainConstants.VOLUNTEER);
        if (role is null)
            return Errors.General.ValueNotFound(DomainConstants.VOLUNTEER);

        using var transaction = await _unitOfWork.BeginTransactionAsync(cancellationToken);

        try
        {
            var roleResult = await _userManager.AddToRoleAsync(user, role.Name);
            if (!roleResult.Succeeded)
                return Errors.General.Failure("Failed to add role to user");

            var volunteerAccount = new VolunteerAccount(user);
            await _accountManager.CreateVolunteerAccount(volunteerAccount);
            
            _logger.LogInformation("Created volunteer account for user with id = {ID}", request.UserId);

            transaction.Commit();

            return volunteerAccount.Id;
        }
        catch (Exception ex)
        {
            transaction.Rollback();
            _logger.LogError(ex, "Failed to create volunteer account");
            return Errors.General.Failure("Transaction failed");
        }
    }
}