using System.Net;
using Blog.Data;
using Blog.Dtos.UserDtos;
using Blog.Extensions;
using Blog.Models;
using Blog.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using SecureIdentity.Password;

namespace Blog.Controllers;

[ApiController]
[Route("/api")]

public class UserController : HomeController
{
    [HttpPost]
    [Route("register")]
    public async Task<IActionResult> RegisterAsync(
        [FromBody] RegisterUserDto modelRequest,
        [FromServices] BlogDataContext context)
    {
        if(!ModelState.IsValid)
            return FailureResponse(
                statusCode: (int)HttpStatusCode.BadRequest,
                ModelState.GetErrors());

        var email = await context.Users.CountAsync(user => user.Email == modelRequest.Email);

        if(email is not 0)
            return FailureResponse(
                statusCode: (int)HttpStatusCode.BadRequest,
                message: "Email already registered. Please, enter a valid email.");

        var user = new User
        {
            Name = modelRequest.Name,
            Email = modelRequest.Email,
            Slug = modelRequest.Email
                    .Replace(oldValue: "@", newValue: "-")
                    .Replace(oldValue: ".", newValue: "-")
        };

        var password = PasswordGenerator.Generate(
            length: 25,
            includeSpecialChars: true,
            upperCase: false);
        
        user.PasswordHash = PasswordHasher.Hash(password: password);

        await context.Users.AddAsync(user);
        await context.SaveChangesAsync();

        return SuccessResponse<dynamic>(
            (int)HttpStatusCode.OK,
            new { Email = user.Email, password});
    }

    [HttpPost]
    [Route("login")]
    public IActionResult Login([FromServices] TokenService tokenService)
    {
        var token = tokenService.GenerateToken(null);

        return Ok(token);
    }
}