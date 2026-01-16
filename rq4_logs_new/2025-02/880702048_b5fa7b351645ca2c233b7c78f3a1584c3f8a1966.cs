using CSharpFunctionalExtensions;
using FluentValidation;
using Microsoft.EntityFrameworkCore;
using P2Project.Accounts.Application.Interfaces;
using P2Project.Accounts.Domain;
using P2Project.Core.Dtos.Accounts;
using P2Project.Core.Extensions;
using P2Project.Core.Interfaces.Queries;
using P2Project.SharedKernel.Errors;

namespace P2Project.Accounts.Application.Queries.GetUserInfoWithAccounts;

public class GetUserInfoWithAccountsHandler :
    IQueryValidationHandler<UserDto, GetUserInfoWithAccountsQuery>
{
    private readonly IAccountsReadDbContext _readDbContext;
    private readonly IValidator<GetUserInfoWithAccountsQuery> _validator;

    public GetUserInfoWithAccountsHandler(
        IAccountsReadDbContext readDbContext,
        IValidator<GetUserInfoWithAccountsQuery> validator)
    {
        _readDbContext = readDbContext;
        _validator = validator;
    }

    public async Task<Result<UserDto, ErrorList>> Handle(
        GetUserInfoWithAccountsQuery query,
        CancellationToken cancellationToken)
    {
        var validationResult = await _validator.ValidateAsync(query, cancellationToken);
        if (validationResult.IsValid == false)
            return validationResult.ToErrorList();
        
        var userDto = await _readDbContext.Users
            .Include(u => u.AdminAccount)
            .Include(u => u.VolunteerAccount)
            .Include(u => u.ParticipantAccount)
            .FirstOrDefaultAsync(u => u.Id == query.Id, cancellationToken);
        
        if (userDto is null)
            return Errors.General.NotFound(query.Id).ToErrorList();
        
        return userDto;
    }
}