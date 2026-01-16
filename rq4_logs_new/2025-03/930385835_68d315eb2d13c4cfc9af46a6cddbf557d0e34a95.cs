using Microsoft.EntityFrameworkCore;
using PetFamily.Accounts.Domain.DataModels;

namespace PetFamily.Accounts.Infrastructure.EntityManagers;

public class AdminAccountManager
{
    private readonly AccountsDbContext _accountsDbContext;

    public AdminAccountManager(AccountsDbContext accountsDbContext)
    {
        _accountsDbContext = accountsDbContext;
    }

    public async Task CreateAdminAccount(AdminAccount adminAccount)
    {
        await _accountsDbContext.AdminAccounts.AddAsync(adminAccount);
        await _accountsDbContext.SaveChangesAsync();
    }
}