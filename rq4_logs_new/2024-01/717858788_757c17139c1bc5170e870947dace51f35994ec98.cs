using MediatR;
using QuizzerApp.Application.Common.Interfaces;
using QuizzerApp.Application.Dtos.Answer;

namespace QuizzerApp.Application.Features.Queries.Answer.ReadAnswersByUserId;

public class ReadAnswerByUserIdQueryHandler : IRequestHandler<ReadAnswersByUserIdQuery, List<AnswerDto>>
{
    private readonly IRepositoryManager _manager;

    public ReadAnswerByUserIdQueryHandler(IRepositoryManager manager)
    {
        _manager = manager;
    }

    public async Task<List<AnswerDto>> Handle(ReadAnswersByUserIdQuery request, CancellationToken cancellationToken)
    {
        var userAnswers = await _manager.Answer.GetAllByUserIdAsync(request.UserId);

        List<AnswerDto> res = new();

        userAnswers.ForEach(ua =>
        {
            res.Add(new AnswerDto(
                Id: ua.Id,
                Text: ua.Text,
                Status: ua.Status,
                UserId: ua.UserId,
                QuestionId: ua.QuestionId.Value.ToString(),
                CreatedDate: ua.CreatedDate,
                UpdatedDate: ua.UpdatedDate
            ));
        });

        return res;
    }
}