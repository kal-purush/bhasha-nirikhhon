using Application.Common.Models;
using Application.Interfaces;
using Domain.Entities;
using MediatR;
using Microsoft.EntityFrameworkCore;

namespace Application.Questions.Commands;

public class DeleteQuestionAndAnswersByIdCommand : IRequest<Result>
{
    public int QuestionId { get; set; }
}

public class DeleteQuestionAndAnswersByIdCommandHandler : IRequestHandler<DeleteQuestionAndAnswersByIdCommand, Result>
{
    private readonly IApplicationDbContext _context;

    public DeleteQuestionAndAnswersByIdCommandHandler(IApplicationDbContext context)
    {
        _context = context;
    }

    public async Task<Result> Handle(DeleteQuestionAndAnswersByIdCommand request, CancellationToken cancellationToken)
    {
        Question? question = await _context.Questions
            .Include(q => q.Options)
            .FirstOrDefaultAsync(q => q.Id == request.QuestionId, cancellationToken);

        if (question == null)
        {
            return Result.Failure("Question not found.");
        }

        _context.Questions.Remove(question);

        await _context.SaveChangesAsync(cancellationToken);

        return Result.Success($"Question with Id {request.QuestionId} and corresponding answers deleted successfully.");
    }
}