using MeowLib.Domain.Team.Services;
using MeowLib.Domain.User.Dto;
using Microsoft.AspNetCore.Mvc.Filters;

namespace MeowLib.WebApi.Filters;

public class RequiredTeamAttribute : Attribute, IAsyncAuthorizationFilter
{
    public async Task OnAuthorizationAsync(AuthorizationFilterContext context)
    {
        var userData = context.HttpContext.Items["UserData"] as UserDto ??
                       throw new NullReferenceException("UserData не установлена");
        var teamService = context.HttpContext.RequestServices.GetRequiredService<ITeamService>();
        var userTeams = await teamService.GetAllUserTeams(userData.Id);
        context.HttpContext.Items.Add("UserTeams", userTeams);
    }
}