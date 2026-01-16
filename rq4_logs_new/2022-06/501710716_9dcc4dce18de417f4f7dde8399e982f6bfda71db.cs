using CollectionManagementAPI.Entity;
using CollectionManagementAPI.Service.Interfeces;

namespace Collection_Management_API.DRY;

public class GetUsersFromToken
{
    public static async Task<UserEntity> GetUserFromToken(HttpContext context, IUserService _userService)
    {
        var id = context.User.Claims
            .FirstOrDefault(x => x.Type == "Id");

        return await _userService.GetById(Int32.Parse(id.Value));
    }
}