using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using QuizSite.Contracts.Database;
using QuizSite.Domain.Database;
using QuizSite.Domain.Interfaces;

namespace QuizSite.Domain.Services;

public class QuizService : IQuizService
{
    private readonly QuizDbContext _dbContext;

    public QuizService(QuizDbContext dbContext)
    {
        _dbContext = dbContext;
    }

    public List<Question> getQuestionsBasedOnCategory(string quizCategory)
    {
        var allQuestions = _dbContext.Questions.ToListAsync();
        var questionsByCategory = allQuestions.Result.Where(x => x.QuizCategory == quizCategory).ToList();
        return questionsByCategory;
    }
}