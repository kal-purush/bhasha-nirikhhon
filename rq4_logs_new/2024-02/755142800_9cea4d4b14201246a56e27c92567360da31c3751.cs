using Application.IServices;
using Application.Models.Dtos;
using Domain.Entities;
using Microsoft.AspNetCore.Mvc;

namespace API.Controllers;

[Route("users")]
public class UsersController : BaseController
{
    private readonly IUserService _userService;
    
    public UsersController(IUserService userService)
    {
        _userService = userService;
    }
    
    [HttpPost("register")]
    public async Task<User> RegisterAsync([FromBody] UserDto register, CancellationToken cancellationToken)
    {
        var result = await _userService.AddUserAsync(register, cancellationToken);
        return result;
    }
}