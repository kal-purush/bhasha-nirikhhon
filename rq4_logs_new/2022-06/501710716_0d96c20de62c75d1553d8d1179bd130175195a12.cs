using CollectionManagementAPI.Entity;
using CollectionManagementAPI.Service.Interfeces;
using Microsoft.AspNetCore.Mvc;
using Microsoft.IdentityModel.Tokens;
using Swashbuckle.AspNetCore.Annotations;

namespace Collection_Management_API.Controllers;

[ApiController]
[SwaggerTag("User")]
public class UserController : Controller
{
    private readonly IUserService _userService;
    private readonly IIdentityService _identityService;

    public UserController(IUserService userService, IIdentityService identityService)
    {
        _userService = userService;
        _identityService = identityService;
    }
    
    [HttpGet("GetUser/{id}")]
    public async Task<UserEntity> GetById(int id) 
    {
        var user = await _userService.GetById(id);
        return user;
    }

    [HttpPost("CreateUser")]
    public async Task<ActionResult<UserEntity>> Create(RegisterModel registerModel)
    {
        UserEntity user = await _userService.SearchByLogin(registerModel.UserName);
        if (user != null)
        {
            return BadRequest("This username is already in use");
        }
        
        _identityService.CreatePasswordHash(registerModel.Password, out byte[] passwordHash);

        user = new UserEntity()
        {
            PasswordHash = passwordHash,
            UserName = registerModel.UserName, 
            Email= registerModel.Email, 
            Name = registerModel?.Name, 
            Surname = registerModel?.Surname
        };
        await _userService.Create(user);
        return Ok(user);
    }
    
    [HttpPut("UpdateUser")]
    public async Task<ActionResult<UserEntity>> Update(UpdateModel updateModel)
    {
        var user = await _userService.GetById(updateModel.Id);
        user.UserName = updateModel.UserName;
        user.Email = updateModel.Email;
        user.Name = updateModel.Name;
        user.Surname = updateModel.Surname;
        
        await _userService.Update(user);
        return Ok(user);
    }
    
    [HttpDelete("DeleteUser")]
    public async Task<bool> Delete(int id)
    {
        return await _userService.Delete(id);
    }

    [HttpGet("SearchUser")]
    public async Task<ActionResult<UserEntity>> SearchByLogin(string login)
    {
        var user = await _userService.SearchByLogin(login);
        if (user == null)
        {
            return NotFound();
        }

        return Ok(user);
    }
}