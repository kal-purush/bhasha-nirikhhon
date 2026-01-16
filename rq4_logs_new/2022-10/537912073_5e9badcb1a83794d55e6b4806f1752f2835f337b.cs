using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using MediatR;
using Microsoft.EntityFrameworkCore;
using QuizSite.Contracts.Database;
using QuizSite.Domain.Database;

namespace QuizSite.Domain.Commands;

public class GetQuizQuestionsCommand : IRequest<GetQuizQuestionsCommandResult>
{
    public string Category { get; init; }
}

public class GetQuizQuestionsCommandResult
{
    public List<Question> Questions { get; init; }
}

public class GetQuizQuestionsCommandHandler : IRequestHandler<GetQuizQuestionsCommand, GetQuizQuestionsCommandResult>
{
    private readonly QuizDbContext _dbContext;

    public GetQuizQuestionsCommandHandler(QuizDbContext dbContext)
    {
        _dbContext = dbContext;
    }

    public async Task<GetQuizQuestionsCommandResult> Handle(GetQuizQuestionsCommand request, CancellationToken cancellationToken)
    {
        var questions = await _dbContext.Questions.Include(x => x.Choises).ToListAsync();
        System.Console.WriteLine(questions);
        var questionsWithCategory = questions.Where(x => x.QuizCategory == request.Category).ToList();
        return new GetQuizQuestionsCommandResult
        {
            Questions = questionsWithCategory
        };
    }
}