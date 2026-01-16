using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using QuizSite.Contracts.Database;
using QuizSite.Domain.Queries;

namespace QuizSite.Domain.Services.Interfaces;

public interface IQuizSiteService
{
    Task<List<Question>> getQuizQuestionsByCategory(GetQuizQuestionsQuery request, CancellationToken cancellationToken);

    Task<List<Result>> getUserResultsByName(GetUserResultsQuery request, CancellationToken cancellationToken);

    Task<int> insertUserResultIntoDb(Result resultToInsert, CancellationToken cancellationToken);
}