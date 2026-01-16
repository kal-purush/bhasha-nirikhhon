using KlockanAPI.Domain.Models;
using KlockanAPI.Infrastructure.Data;
using KlockanAPI.Infrastructure.Extensions;
using KlockanAPI.Infrastructure.Repositories.Interfaces;
using Microsoft.EntityFrameworkCore;

namespace KlockanAPI.Infrastructure;

public class ClassroomUserRepository : IClassroomUserRepository
{
    private readonly KlockanContext _context;

    public ClassroomUserRepository(KlockanContext context)
    {
        _context = context;
    }

    public async Task<IEnumerable<ClassroomUser>> GetClassroomUsersAsync(int classroomId)
    {
        var users = _context.ClassroomUsers
            .AsNoTracking()
            .Where(u => u.ClassroomId == classroomId).ToList();

        return await Task.FromResult(users);
    }

    public async Task<IEnumerable<ClassroomUser>> UpdateClassroomUsersAsync(int classroomId, IEnumerable<ClassroomUser> classroomUsers)
    {
        // List of valid roles
        var validRoleNames = new List<string>() { "Trainer", "Student", "Guest" };
        var validRoleIds = _context.Roles
            .AsNoTracking()
            .Where(role => validRoleNames.Any(validRole => validRole.Contains(role.Name)))
            .Select(role => role.Id)
            .ToList();

        // Clean invalid users 
        var incomingUsers = _context.Users
            .AsNoTracking()
            .AsEnumerable()
            .Where(user => classroomUsers.Any((incomingUser) =>
                incomingUser.UserId == user.Id
                && validRoleIds.Any(roleId => roleId == incomingUser.RoleId)
            ))
            .Select((user) => classroomUsers.First(incomingUser => incomingUser.UserId == user.Id))
            .ToList();

        // List of database users
        var databaseUsers = await GetClassroomUsersAsync(classroomId);

        var usersToDelete = incomingUsers.FilterTarget(
            databaseUsers,
            // List of `database users` not present in `incoming users`
            (master, target) => target.Id == master.Id,
            false
        );

        var usersToUpdate =
            databaseUsers.Count() == 0
                ? incomingUsers.Where(user => user.Id == 0).ToList()
                : incomingUsers.FilterByTarget(
                    databaseUsers,
                    // list of `incoming users` present in `database users` or `incoming users` with id equal to zero (new users)
                    (master, target) => target.Id == master.Id || master.Id == 0
                );

        // Update context
        _context.ClassroomUsers.UpdateRange(usersToUpdate);
        _context.ClassroomUsers.RemoveRange(usersToDelete);

        await _context.SaveChangesAsync();

        // Get updated list of users  
        return await GetClassroomUsersAsync(classroomId);
    }
}