using Jared.Presentation.Requests.Roles.List;
using Jared.Presentation.Requests.User.List;
using Jared.Presentation.Requests.User.UpdateRole;
using Jared.Shared.Dtos.UserDtos;

namespace Jared.Presentation.Pages;

public partial class Users
{
    private List<UserListDto> users = new();
    private Dictionary<int, string> roles = new();

    protected override async Task OnInitializedAsync()
    {
        await getUsersAsync();
        await getRolesAsync();
    }

    private async Task getUsersAsync()
    {
        var result = await Mediator.Send(new UserListQuery());

        if (!result.Success)
        {
            Console.WriteLine("Error when get users list");
            return;
        }

        users = result.Data;
    }

    private async Task getRolesAsync()
    {
        var result = await Mediator.Send(new RoleListQuery());

        if (!result.Success)
        {
            Console.WriteLine("Error when get roles list");
            return;
        }

        roles = result.Data.ToDictionary(x => x.Id, x => x.Name);
    }

    private async Task save(UserListDto user)
    {
        var result = await Mediator.Send(new UserRoleUpdateCommand(new()
        {
            Id = user.Id,
            RoleId = user.RoleId,
        }));

        if (!result.Success)
        {
            Console.WriteLine("Update user role failed");
        }
    }
}