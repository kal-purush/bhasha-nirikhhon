using PetFamily.Accounts.Application.Abstractions;
using PetFamily.Accounts.Domain.DataModels;

namespace PetFamily.Accounts.Infrastructure.EntityManagers;

public class VolunteerAccountManager : IVolunteerAccountManager
{
    private readonly AccountsDbContext _accountsDbContext;

    public VolunteerAccountManager(AccountsDbContext accountsDbContext)
    {
        _accountsDbContext = accountsDbContext;
    }

    public async Task CreateVolunteerAccount(VolunteerAccount volunteerAccount)
    {
        await _accountsDbContext.VolunteerAccounts.AddAsync(volunteerAccount);
        await _accountsDbContext.SaveChangesAsync();
    }
}