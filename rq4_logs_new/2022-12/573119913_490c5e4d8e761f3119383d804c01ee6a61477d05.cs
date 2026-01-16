using System.Security.Claims;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Internal;
using VanKassa.Backend.Infrastructure.Data;
using VanKassa.Domain.Models;

namespace VanKassa.Backend.Core.Services;

public class CookieIdentity
{
    private readonly IDbContextFactory<VanKassaDbContext> _dbContextFactory;

    public CookieIdentity(IDbContextFactory<VanKassaDbContext> dbContextFactory)
    {
        _dbContextFactory = dbContextFactory;
    }


    public async Task<(bool canLogin, ClaimsPrincipal usersClaims)> CanLogin(Login loginData)
    {
        try
        {
            await using var dbContext = await _dbContextFactory.CreateDbContextAsync();

            var user = await dbContext.UsersCredentials.Join(dbContext.Users,
                    uc => uc.UserId, u => u.UserId, (uo, u) =>
                        new
                        {
                            userName = uo.UserName,
                            password = uo.Password,
                            userId = u.UserId,
                            roleId = u.RoleId
                        }
                )
                .Join(dbContext.Roles, ui => ui.roleId, r => r.RoleId, (ui, r) =>
                    new
                    {
                       ui = ui,
                        roleName = r.Name
                    })
                .FirstOrDefaultAsync((us =>
                    us.ui.userName == loginData.UserName || us.ui.password == loginData.Password));
            
            if (user is null)
                return (false, new ClaimsPrincipal());

            return (true, BuildUserClaimsPrinciples(user.ui.userName, user.roleName));

        }
        catch (InvalidOperationException ex)
        {
            throw new InvalidOperationException(ex.Message);
        }
    }

    private ClaimsPrincipal BuildUserClaimsPrinciples(string userName, string roleName)
    {
        var claims = new List<Claim>
        {
            new(ClaimTypes.Name, userName),
            new(ClaimTypes.Role, roleName)
        };

        var identity = new ClaimsIdentity(claims, CookieAuthenticationDefaults.AuthenticationScheme);
        return new ClaimsPrincipal(identity);
    }
}