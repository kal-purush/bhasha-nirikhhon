using QuizzerApp.Application.Common.Interfaces;
using QuizzerApp.Infrastructure.EFCore.Contexts;

namespace QuizzerApp.Infrastructure.Persistence;

public class RepositoryManager : IRepositoryManager
{
    private readonly Lazy<IQuestionRepository> _questionRepository;
    private readonly QuizzerAppContext _context;

    public RepositoryManager(QuizzerAppContext context)
    {
        _context = context;
        _questionRepository = new Lazy<IQuestionRepository>(() => new QuestionRepository(context: _context));
    }

    public IQuestionRepository Question => _questionRepository.Value;

    public Task SaveAsync() => _context.SaveChangesAsync();
}