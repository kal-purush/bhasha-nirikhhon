using CosmeticsStore.Services.Constants;
using CosmeticsStore.Services.DTO.Request;
using CosmeticsStore.Services.Interfaces;
using Microsoft.AspNetCore.Mvc;

namespace CosmeticsStore.API.Controllers;

[ApiController]
[Route("api/[controller]")]
public class UsersController : ControllerBase
{
    private readonly IUserService _userService;

    public UsersController(IUserService userService)
    {
        _userService = userService;
    }

    [HttpPost]
    [Route("Register")]
    public async Task<IActionResult> RegiterAsync([FromBody] CustomerRegisterDTO requestBody)
    {
        var (serviceResponse, token) = await _userService.RegisterAsync(requestBody, UserConstants.CustomerRole);
        if (!serviceResponse.Succeeded)
        {
            return StatusCode(serviceResponse.StatusCode, new { status = serviceResponse.Status, details = serviceResponse.Details });
        }

        return Ok(new { status = serviceResponse.Status, details = serviceResponse.Details });
    }


}