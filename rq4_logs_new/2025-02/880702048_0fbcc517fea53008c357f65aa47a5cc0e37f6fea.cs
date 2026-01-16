using Microsoft.EntityFrameworkCore;
using P2Project.Accounts.Application.Interfaces;
using P2Project.Core.Interfaces.Queries;

namespace P2Project.Accounts.Application.Queries.IsAnyAdminAccountByUserId;

public class IsAnyAdminAccountByUserIdHandler :
    IQueryHandler<bool, IsAnyAdminAccountByUserIdQuery>
{
    private readonly IAccountsReadDbContext _readDbContext;

    public IsAnyAdminAccountByUserIdHandler(IAccountsReadDbContext readDbContext)
    {
        _readDbContext = readDbContext;
    }

    public async Task<bool> Handle(
        IsAnyAdminAccountByUserIdQuery query,
        CancellationToken cancellationToken)
    {
        return await _readDbContext.AdminAccounts.AnyAsync(a =>
            a.UserId == query.UserId, cancellationToken);
    }
}