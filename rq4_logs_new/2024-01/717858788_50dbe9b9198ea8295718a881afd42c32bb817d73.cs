using QuizzenApp.Domain.Entities.AnswerAggregate;
using QuizzerApp.Application.Common.Interfaces;
using QuizzerApp.Infrastructure.Contracts;
using QuizzerApp.Infrastructure.EFCore.Contexts;

namespace QuizzerApp.Infrastructure.Persistence;

public class AnswerRepository(QuizzerAppContext context) : RepositoryBase<Answer>(context), IAnswerRepository
{
    public async Task CreateAsync(Answer answer) => Create(answer);

    public void DeleteAsync(Answer answer)
    {
        throw new NotImplementedException();
    }

    public Task<List<Answer>> GetAllAsync()
    {
        throw new NotImplementedException();
    }

    public Task<Answer> GetAsync(Guid id)
    {
        throw new NotImplementedException();
    }

    public void UpdateAsync(Answer answer)
    {
        throw new NotImplementedException();
    }
}