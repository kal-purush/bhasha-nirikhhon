using MediatR;
using QuizzerApp.Application.Common.Interfaces;
using QuizzerApp.Application.Dtos.Answer;

namespace QuizzerApp.Application.Features.Queries.Answer.ReadAnswerById;

public class ReadAnswerByIdQueryHandler : IRequestHandler<ReadAnswerByIdQuery, AnswerDto>
{
    private readonly IRepositoryManager _manager;

    public ReadAnswerByIdQueryHandler(IRepositoryManager manager)
    {
        _manager = manager;
    }

    public async Task<AnswerDto> Handle(ReadAnswerByIdQuery request, CancellationToken cancellationToken)
    {
        var answer = await _manager.Answer.GetAsync(request.AnswerId);

        AnswerDto res = new(
            Id: answer.Id,
            Text: answer.Text,
            Status: answer.Status,
            UserId: answer.UserId,
            QuestionId: answer.QuestionId.Value.ToString(),
            CreatedDate: answer.CreatedDate,
            UpdatedDate: answer.UpdatedDate
        );

        return res;
    }
}