using Application.Common.Models;
using MediatR;
using Domain.Entities;
using Application.Interfaces;
using Domain.Enum;
using Microsoft.EntityFrameworkCore;
using Microsoft.AspNetCore.Identity;

public class AssignTaskToStudentCommand : IRequest<Result>
{
    public string StudentId { get; set; }
    public int ModuleTaskId { get; set; }
}

public class AssignTaskToStudentCommandHandler : IRequestHandler<AssignTaskToStudentCommand, Result>
{
    private readonly IApplicationDbContext _context;
    private readonly UserManager<Student> _userManager;

    public AssignTaskToStudentCommandHandler(IApplicationDbContext context, UserManager<Student> userManager)
    {
        _context = context;
        _userManager = userManager;
    }

    public async Task<Result> Handle(AssignTaskToStudentCommand request, CancellationToken cancellationToken)
    {
        //var student = await _userManager.FindByIdAsync(request.StudentId);
        Student? student = await _userManager.Users.Include(s => s.AssignedTasks)
                                 .FirstOrDefaultAsync(s => s.Id == request.StudentId, cancellationToken);

        if (student == null)
        {
            return Result.Failure($"Student with ID {request.StudentId} not found.");
        }

        //_context.Entry(student).Collection(s => s.AssignedTasks).Load();



        // Check if the task exists
        var task = await _context.Tasks.FindAsync(request.ModuleTaskId, cancellationToken);
        if (task == null)
        {
            return Result.Failure($"ModuleTask with ID {request.ModuleTaskId} not found.");
        }

        student.AssignedTasks ??= new List<ModuleTask>(); // Initialize if null

        if (student.AssignedTasks.Any(t => t.Id == request.ModuleTaskId))
        {
            return Result.Failure($"Student with ID {request.StudentId} is already assigned this task.");
        }

        student.AssignedTasks.Add(task);

        await _userManager.UpdateAsync(student);
        //_context.Students.Update(student);
        await _context.SaveChangesAsync(cancellationToken);

        return Result.Success("");
    }
}