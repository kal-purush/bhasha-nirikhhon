using System.Net.Mime;
using GiftSuggester.Core.Users.Models;
using GiftSuggester.Core.Users.Services;
using GiftSuggester.Web.Users.Models;
using Microsoft.AspNetCore.Mvc;

namespace GiftSuggester.Web.Users.Controllers;

[ApiController]
[Route("users")]
[Produces(MediaTypeNames.Application.Json)]
public class UserController
{
    private readonly IUserService _userService;

    public UserController(IUserService userService)
    {
        _userService = userService;
    }

    [HttpPost]
    public Task AddAsync(UserCreationRequest creationRequest, CancellationToken cancellationToken)
    {
        var creationModel = new UserCreationModel
        {
            Name = creationRequest.Name,
            Login = creationRequest.Login,
            Email = creationRequest.Email,
            Password = creationRequest.Password,
            DateOfBirth = creationRequest.DateOfBirth
        };

        return _userService.AddAsync(creationModel, cancellationToken);
    }

    [HttpGet("{id:guid}")]
    public async Task<UserResponse> GetByIdAsync(Guid id, CancellationToken cancellationToken)
    {
        var user = await _userService.GetByIdAsync(id, cancellationToken);

        return new UserResponse
        {
            Id = user.Id,
            Name = user.Name,
            Login = user.Login,
            Email = user.Email,
            DateOfBirth = user.DateOfBirth,
            GroupIds = user.GroupIds
        };
    }

    [HttpPut]
    public Task UpdateAsync(UserUpdateRequest updateRequest, CancellationToken cancellationToken)
    {
        return _userService.UpdateAsync(
            new User
            {
                Id = updateRequest.Id,
                Name = updateRequest.Name,
                Login = updateRequest.Login,
                Email = updateRequest.Email,
                Password = updateRequest.Password,
                DateOfBirth = updateRequest.DateOfBirth
            }, cancellationToken);
    }

    [HttpDelete("{id:guid}")]
    public Task RemoveByIdAsync(Guid id, CancellationToken cancellationToken)
    {
        return _userService.RemoveByIdAsync(id, cancellationToken);
    }
}