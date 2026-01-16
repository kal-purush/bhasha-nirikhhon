using Jared.Application.Services.User;
using Jared.Domain.Models;
using Jared.Shared.Abstractions;
using Jared.Shared.Interfaces;
using MediatR;
using Microsoft.EntityFrameworkCore;

namespace Jared.Application.Requests.Users.UpdateRole;

public class UserRoleUpdateCommandHandler : IRequestHandler<UserRoleUpdateCommand, Result<bool>>
{
    private readonly IDataContext dataContext;
    private readonly IUserService userService;

    public UserRoleUpdateCommandHandler(
        IDataContext dataContext,
        IUserService userService)
    {
        this.dataContext = dataContext;
        this.userService = userService;
    }

    public async Task<Result<bool>> Handle(UserRoleUpdateCommand request, CancellationToken cancellationToken)
    {
        try
        {
            var user = await dataContext
                .Set<User>()
                .Include(x => x.Role)
                .FirstOrDefaultAsync(x => x.Id == userService.GetUser().Id, cancellationToken);

            if (user is null)
            {
                return Result.Fail<bool>("User not found");
            }

            if (user.Id == request.dto.Id && user.Role?.Name == "Admin")
            {
                return Result.Fail<bool>("You can't take away your admin rights");
            }

            user.RoleId = request.dto.RoleId;

            await dataContext.SaveChangesAsync(cancellationToken);

            return Result.Ok(true);
        }
        catch (Exception ex)
        {
            return Result.Fail<bool>(ex.Message);
        }
    }
}