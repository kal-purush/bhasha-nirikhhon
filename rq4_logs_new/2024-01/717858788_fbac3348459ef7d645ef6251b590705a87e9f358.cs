using Microsoft.EntityFrameworkCore;
using QuizzenApp.Domain.Entities.UserAggregate;
using QuizzerApp.Application.Common.Interfaces;
using QuizzerApp.Infrastructure.Contracts;
using QuizzerApp.Infrastructure.EFCore.Contexts;

namespace QuizzerApp.Infrastructure.Persistence;

public class UserRepository(QuizzerAppContext context) : RepositoryBase<User>(context), IUserRepository
{

    public async Task CreateAsync(User user) => Create(user);

    public async Task DeleteAsync(User user) => Delete(user);

    public async Task<User> GetAsync(string id) => await FindByCondition(q => q.Id == id, false).FirstOrDefaultAsync();

    public async Task UpdateAsync(User user) => Update(user);
}