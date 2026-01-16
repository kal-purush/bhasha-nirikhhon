using Application.Common.Models;
using Application.Interfaces;
using Domain.Entities;
using MediatR;
using Domain.Enum;
using Microsoft.AspNetCore.Identity;
using Microsoft.EntityFrameworkCore;

namespace Application.Tasks.Commands;
public class SubmitTaskCommand : IRequest<Result>
{
    public int TaskId { get; set; }
    public required string StudentId { get; set; }
    public required string SubmissionLink { get; set; }
}

public class SubmitTaskCommandHandler(IApplicationDbContext context, UserManager<Student> userManager) : IRequestHandler<SubmitTaskCommand, Result>
{
    private readonly IApplicationDbContext _context = context;
    private readonly UserManager<Student> _userManager = userManager;

    public async Task<Result> Handle(SubmitTaskCommand request, CancellationToken cancellationToken)
    {
        //Student? student = await _userManager.FindByIdAsync(request.StudentId);
        Student? student = await _userManager.Users.Include(s => s.AssignedTasks)
                                 .FirstOrDefaultAsync(s => s.Id == request.StudentId, cancellationToken);
        if (student == null)
        {
            return Result.Failure($"Student with ID {request.StudentId} not found.");
        }

        ModuleTask? task = await _context.Tasks.FindAsync(request.TaskId, cancellationToken);
        if (task == null)
        {
            return Result.Failure($"Task with ID {request.TaskId} not found.");
        }

        // Check if the student is assigned this task
        if (!student.AssignedTasks.Any(t => t.Id == request.TaskId))
        {
            return Result.Failure($"Student with ID {request.StudentId} is not assigned to this task.");
        }


        SubmissionDetails? submission = await _context.SubmissionDetails.FirstOrDefaultAsync(s => s.TaskId == request.TaskId
                                                    && s.StudentId == request.StudentId);

        if (submission == null)
        {
            submission = new()
            {
                TaskId = request.TaskId,
                TaskStatus = ModuleTaskStatus.Submitted,
                TaskStatusDesc = ModuleTaskStatus.Submitted.ToString(),
                StudentId = request.StudentId,
                SubmissionLink = request.SubmissionLink,
                SubmissionDate = DateTime.Now
            };
            _context.SubmissionDetails.Add(submission);
        }

        submission.TaskStatus = ModuleTaskStatus.Submitted;
        submission.TaskStatusDesc = ModuleTaskStatus.Submitted.ToString();
        submission.SubmissionLink = request.SubmissionLink;
        submission.SubmissionDate = DateTime.Now;
        _context.SubmissionDetails.Update(submission);

        await _context.SaveChangesAsync(cancellationToken);

        return Result.Success("Task submitted successfully.");
    }
}