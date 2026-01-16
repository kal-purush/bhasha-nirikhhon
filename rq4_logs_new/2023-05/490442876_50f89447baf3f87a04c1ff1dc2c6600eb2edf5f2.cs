using System.Net.Mime;
using GiftSuggester.Core.Groups.Models;
using GiftSuggester.Core.Groups.Services;
using GiftSuggester.Web.Groups.Models;
using GiftSuggester.Web.Users.Models;
using Microsoft.AspNetCore.Mvc;

namespace GiftSuggester.Web.Groups.Controllers;

[ApiController]
[Route("groups")]
[Produces(MediaTypeNames.Application.Json)]
public class GroupController
{
    private readonly IGroupService _groupService;

    public GroupController(IGroupService groupService)
    {
        _groupService = groupService;
    }

    [HttpPost]
    public Task AddAsync(GroupCreationRequest creationRequest, CancellationToken cancellationToken)
    {
        return _groupService.AddAsync(
            new GroupCreationModel
            {
                OwnerId = creationRequest.OwnerId
            }, cancellationToken);
    }

    [HttpPut("{groupId:guid}/{userId:guid}")]
    public Task AddUserToGroupAsync(Guid groupId, Guid userId, CancellationToken cancellationToken)
    {
        return _groupService.AddUserToGroupAsync(groupId, userId, cancellationToken);
    }

    [HttpGet("{id:guid}")]
    public async Task<GroupResponse> GetByIdAsync(Guid id, CancellationToken cancellationToken)
    {
        var group = await _groupService.GetByIdAsync(id, cancellationToken);

        return new GroupResponse
        {
            Id = group.Id,
            OwnerId = group.OwnerId,
            Members = group.Members
                .Select(
                    user => new UserResponse
                    {
                        Id = user.Id,
                        Name = user.Name,
                        Login = user.Login,
                        Email = user.Email,
                        DateOfBirth = user.DateOfBirth,
                        GroupIds = user.GroupIds
                    })
                .ToList()
        };
    }

    [HttpDelete("{id:guid}")]
    public Task RemoveByIdAsync(Guid id, CancellationToken cancellationToken)
    {
        return _groupService.RemoveByIdAsync(id, cancellationToken);
    }
}