using Application.Common.Models;
using MediatR;
using Application.Interfaces;
using Domain.Entities;
using Domain.Enum;
using Microsoft.EntityFrameworkCore;

namespace Application.Quizzes.Commands
{
    public class UpdateQuizStatusCommand : IRequest<Result>
    {
        public int QuizId { get; set; }
        
        public string NewStatus { get; set; }
    }

    public class UpdateQuizStatusCommandHandler : IRequestHandler<UpdateQuizStatusCommand, Result>
    {
        private readonly IApplicationDbContext _context;

        public UpdateQuizStatusCommandHandler(IApplicationDbContext context)
        {
            _context = context;
        }

        public async Task<Result> Handle(UpdateQuizStatusCommand request, CancellationToken cancellationToken)
        {
            Quiz? quiz = await _context.Quizzes.FirstOrDefaultAsync(q => q.Id == request.QuizId, cancellationToken);

            if (quiz == null)
            {
                return Result.Failure($"Quiz with ID {request.QuizId} not found.");
            }
            
            
            // Convert string to ModuleStatus enum
            if (!Enum.TryParse(request.NewStatus, true, out QuizStatus status))
            {
                return Result.Failure("Invalid module status.");
            }

            quiz.QuizStatus = status;
            quiz.QuizStatusDes = request.NewStatus;

            await _context.SaveChangesAsync(cancellationToken);

            return Result.Success("Quiz status updated successfully.");
        }
    }
}