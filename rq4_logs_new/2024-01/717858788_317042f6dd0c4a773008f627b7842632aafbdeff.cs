using MediatR;
using Microsoft.EntityFrameworkCore;
using QuizzerApp.Application.Common.Interfaces;
using QuizzerApp.Application.Dtos.Question;

namespace QuizzerApp.Application.Features.Queries.Question.ReadQuestions;

public class ReadQuestionQueryHandler : IRequestHandler<ReadQuestionQuery, List<QuestionDto>>
{
    private readonly IRepositoryManager _manager;

    public ReadQuestionQueryHandler(IRepositoryManager manager)
    {
        _manager = manager;
    }

    public async Task<List<QuestionDto>> Handle(ReadQuestionQuery request, CancellationToken cancellationToken)
    {
        var query = _manager.Question.GetQueriable();

        if (!string.IsNullOrEmpty(request.Exam))
            query = query.Where(q => string.Equals(q.Exam.Name, request.Exam));

        if (!string.IsNullOrEmpty(request.Subject))
            query = query.Where(q => string.Equals(q.Subject.Name, request.Subject));

        if (!string.IsNullOrEmpty(request.Topic))
            query = query.Where(q => string.Equals(q.Topic.Name, request.Topic));

        var questions = await query.ToListAsync();

        List<QuestionDto> res = new();

        questions.ForEach(q =>
        {

            res.Add(new QuestionDto(
                Id: q.Id,
                Title: q.Title,
                Description: q.Description,
                Status: q.Status.ToString(),
                ownerId: q.UserId,
                ExamId: q.ExamId,
                SubjectId: q.SubjectId,
                TopicId: q.TopicId,
                CreatedDate: q.CreatedDate
            ));
        });

        return res;
    }
}