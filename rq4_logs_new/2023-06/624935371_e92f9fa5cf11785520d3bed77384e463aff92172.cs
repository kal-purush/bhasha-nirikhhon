using Microsoft.AspNetCore.Identity;

namespace Hotexper.Domain.Repositories;

public interface IRoleRepository
{
    Task CreateAsync(List<IdentityRole> roles);
}