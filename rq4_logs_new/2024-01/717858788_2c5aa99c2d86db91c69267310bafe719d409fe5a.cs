using MediatR;
using Microsoft.EntityFrameworkCore;
using QuizzenApp.Domain.Entities.QuestionAggregate.ValueObjects;
using QuizzerApp.Application.Common.Interfaces;
using QuizzerApp.Application.Dtos.Answer;

namespace QuizzerApp.Application.Features.Queries.Answer.ReadAnswers;

public record ReadAnswersQuery(string? QuestionId, string? userId = null) : IRequest<List<AnswerDto>>;



public class ReadAnswersQueryHandler : IRequestHandler<ReadAnswersQuery, List<AnswerDto>>
{
    private readonly IRepositoryManager _manager;

    public ReadAnswersQueryHandler(IRepositoryManager manager)
    {
        _manager = manager;
    }

    public async Task<List<AnswerDto>> Handle(ReadAnswersQuery request, CancellationToken cancellationToken)
    {
        var query = _manager.Answer.GetQueriable();

        if (!string.IsNullOrEmpty(request.QuestionId))
            query = query.Where(a => a.QuestionId == new QuestionId(new Guid(request.QuestionId)));

        if (!string.IsNullOrEmpty(request.userId))
        {
            var userId = request.userId;

            query = query.Where(a => a.UserId == userId);
        }

        var answers = await query.ToListAsync(cancellationToken);


        List<AnswerDto> res = new();

        answers.ForEach(a =>
        {
            res.Add(new AnswerDto(
                Id: a.Id,
                Text: a.Text,
                Status: a.Status,
                UserId: a.UserId,
                QuestionId: a.QuestionId.Value.ToString(),
                CreatedDate: a.CreatedDate,
                UpdatedDate: a.UpdatedDate
            ));
        });

        return res;
    }
}