using GiftSuggester.Core.Exceptions;
using GiftSuggester.Core.Users.Models;
using GiftSuggester.Core.Users.Repositories;
using GiftSuggester.Data.Users.Models;
using Microsoft.EntityFrameworkCore;

namespace GiftSuggester.Data.Users.Repositories;

public class UserRepository : IUserRepository
{
    private readonly GiftSuggesterContext _context;

    public UserRepository(GiftSuggesterContext context)
    {
        _context = context;
    }

    public Task AddAsync(User user, CancellationToken cancellationToken)
    {
        return _context.Users
            .AddAsync(
                new UserDbModel
                {
                    Id = user.Id,
                    Name = user.Name,
                    Login = user.Login,
                    Email = user.Email,
                    Password = user.Password,
                    DateOfBirth = user.DateOfBirth,
                    Groups = user.GroupIds
                        .Select(id => _context.Groups.First(group => group.Id == id))
                        .ToList()
                }
                , cancellationToken)
            .AsTask();
    }

    public async Task<User> GetByIdAsync(Guid id, CancellationToken cancellationToken)
    {
        var dbModel =
            await _context.Users.FirstOrDefaultAsync(model => model.Id == id, cancellationToken) ??
            throw new EntityNotFoundException($"User with id: {id} is not found!");

        return new User
        {
            Id = dbModel.Id,
            Name = dbModel.Name,
            Login = dbModel.Login,
            Email = dbModel.Email,
            Password = dbModel.Password,
            DateOfBirth = dbModel.DateOfBirth,
            GroupIds = dbModel.Groups
                .Select(model => model.Id)
                .ToList()
        };
    }

    public async Task UpdateAsync(User user, CancellationToken cancellationToken)
    {
        var dbModel =
            await _context.Users.FirstOrDefaultAsync(model => model.Id == user.Id, cancellationToken) ??
            throw new EntityNotFoundException($"User with id: {user.Id} is not found!");

        dbModel.Name = user.Name;
        dbModel.Login = user.Login;
        dbModel.Email = user.Email;
        dbModel.Password = user.Password;
        dbModel.DateOfBirth = user.DateOfBirth;
        dbModel.Groups = user.GroupIds
            .Select(id => _context.Groups.First(group => group.Id == id))
            .ToList();
    }

    public async Task RemoveByIdAsync(Guid id, CancellationToken cancellationToken)
    {
        var dbModel =
            await _context.Users.FirstOrDefaultAsync(model => model.Id == id, cancellationToken) ??
            throw new EntityNotFoundException($"User with id: {id} is not found!");

        _context.Users.Remove(dbModel);
    }
}