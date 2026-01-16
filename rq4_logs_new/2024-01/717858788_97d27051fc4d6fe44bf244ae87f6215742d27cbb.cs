using MediatR;
using QuizzerApp.Application.Common.Interfaces;
using QuizzerApp.Application.Dtos.Question;

namespace QuizzerApp.Application.Features.Queries.Question.ReadQuestionByUserId;

public class ReadQuestionsByUserIdQueryHandler : IRequestHandler<ReadQuestionsByUserIdQuery, List<QuestionDto>>
{
    private readonly IRepositoryManager _manager;

    public ReadQuestionsByUserIdQueryHandler(IRepositoryManager manager)
    {
        _manager = manager;
    }

    public async Task<List<QuestionDto>> Handle(ReadQuestionsByUserIdQuery request, CancellationToken cancellationToken)
    {
        var userQuestions = await _manager.Question.GetAllByUserIdAsync(request.UserId);

        List<QuestionDto> res = new();

        userQuestions.ForEach(uq =>
        {

            res.Add(new QuestionDto(
                Id: uq.Id,
                Title: uq.Title,
                Description: uq.Description,
                Status: uq.Status.ToString(),
                ownerId: uq.UserId,
                ExamId: uq.ExamId,
                SubjectId: uq.SubjectId,
                TopicId: uq.TopicId,
                CreatedDate: uq.CreatedDate
            ));
        });

        return res;
    }
}