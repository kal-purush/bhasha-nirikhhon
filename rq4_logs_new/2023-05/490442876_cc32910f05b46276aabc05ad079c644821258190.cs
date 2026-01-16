using GiftSuggester.Core.Exceptions;
using GiftSuggester.Core.Groups.Models;
using GiftSuggester.Core.Groups.Repositories;
using GiftSuggester.Core.Users.Models;
using GiftSuggester.Data.Groups.Models;
using Microsoft.EntityFrameworkCore;

namespace GiftSuggester.Data.Groups.Repositories;

public class GroupRepository : IGroupRepository
{
    private readonly GiftSuggesterContext _context;

    public GroupRepository(GiftSuggesterContext context)
    {
        _context = context;
    }

    public Task AddAsync(Group group, CancellationToken cancellationToken)
    {
        return _context.Groups
            .AddAsync(
                new GroupDbModel
                {
                    Id = group.Id,
                    OwnerId = group.OwnerId,
                    Members = group.Members
                        .Select(member => _context.Users.First(user => user.Id == member.Id))
                        .ToList()
                }, cancellationToken)
            .AsTask();
    }

    public async Task AddUserToGroupAsync(Guid groupId, Guid userId, CancellationToken cancellationToken)
    {
        var userDbModel =
            await _context.Users.FirstOrDefaultAsync(dbModel => dbModel.Id == userId, cancellationToken) ??
            throw new EntityNotFoundException($"User with id: {userId} is not found!");

        var groupDbModel =
            await _context.Groups.FirstOrDefaultAsync(dbModel => dbModel.Id == groupId, cancellationToken) ??
            throw new EntityNotFoundException($"Group with id: {groupId} is not found!");

        groupDbModel.Members.Add(userDbModel);
    }

    public async Task<Group> GetByIdAsync(Guid id, CancellationToken cancellationToken)
    {
        var dbModel = await _context.Groups.FirstOrDefaultAsync(dbModel => dbModel.Id == id, cancellationToken) ??
                      throw new EntityNotFoundException($"Group with id: {id} is not found!");

        return new Group
        {
            Id = dbModel.Id,
            OwnerId = dbModel.OwnerId,
            Members = dbModel.Members
                .Select(
                    member => new User
                    {
                        Id = member.Id,
                        Name = member.Name,
                        Login = member.Login,
                        Email = member.Email,
                        Password = member.Password,
                        DateOfBirth = member.DateOfBirth,
                        GroupIds = member.Groups
                            .Select(groupDbModel => groupDbModel.Id)
                            .ToList()
                    })
                .ToList()
        };
    }

    public async Task RemoveByIdAsync(Guid id, CancellationToken cancellationToken)
    {
        var dbModel = await _context.Groups.FirstOrDefaultAsync(dbModel => dbModel.Id == id, cancellationToken) ??
                      throw new EntityNotFoundException($"Group with id: {id} is not found!");

        _context.Groups.Remove(dbModel);
    }
}