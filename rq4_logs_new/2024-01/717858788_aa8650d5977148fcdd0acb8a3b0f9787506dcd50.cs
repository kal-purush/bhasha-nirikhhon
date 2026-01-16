using MediatR;
using QuizzerApp.Application.Common.Interfaces;
using QuizzerApp.Application.Dtos.Answer;

namespace QuizzerApp.Application.Features.Queries.Answer.ReadAnswersByQuestionId;

public class ReadAnswerByQuestionIdQueryHandler : IRequestHandler<ReadAnsersByQuestionIdQuery, List<AnswerDto>>
{
    private readonly IRepositoryManager _manager;

    public ReadAnswerByQuestionIdQueryHandler(IRepositoryManager manager)
    {
        _manager = manager;
    }

    public async Task<List<AnswerDto>> Handle(ReadAnsersByQuestionIdQuery request, CancellationToken cancellationToken)
    {
        var questionAnswers = await _manager.Answer.GetAllByQuestionIdAsync(request.QuestionId);

        List<AnswerDto> res = new();
        questionAnswers.ForEach(qa =>
        {
            res.Add(new AnswerDto(
                Id: qa.Id,
                Text: qa.Text,
                Status: qa.Status,
                UserId: qa.UserId,
                QuestionId: qa.QuestionId.Value.ToString(),
                CreatedDate: qa.CreatedDate,
                UpdatedDate: qa.UpdatedDate
            ));
        });

        return res;
    }
}