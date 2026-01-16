using Application.Common.Models;
using MediatR;
using Application.Interfaces;
using Domain.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.AspNetCore.Identity;
using Domain.Enum;
using System.Linq;

namespace Application.Dashboard.Queries;
public class GetPerformanceAnalysisQuery : IRequest<Result>
{
    public required string StudentId;
}

public class GetPerformanceAnalysisQueryHandler : IRequestHandler<GetPerformanceAnalysisQuery, Result>
{
    private readonly IApplicationDbContext _context;
    private readonly UserManager<Student> _userManager;

    public GetPerformanceAnalysisQueryHandler(IApplicationDbContext context, UserManager<Student> userManager)
    {
        _context = context;
        _userManager = userManager;
    }

    public async Task<Result> Handle(GetPerformanceAnalysisQuery request, CancellationToken cancellationToken)
    {
        Student? student = await _userManager.Users.Include(s => s.AssignedTasks)
                                 .FirstOrDefaultAsync(s => s.Id == request.StudentId, cancellationToken);
        if (student == null)
        {
            // Handle student not found scenario
            return Result.Failure("student Not found");
        }

        List<PerformanceWeek> weeks = new List<PerformanceWeek>();

        for (int i=0; i<8; i++)
        {
            DateTime WeeksAgo = DateTime.UtcNow.Subtract(TimeSpan.FromDays(7 * i));
            weeks.Add(
                new()
                {
                    StudentScore = await GetStudentScore(student.Id, WeeksAgo),
                    AverageScore = 1
                }
                );
        }
        PerformanceAnalysis performanceAnalysis = new()
        {
            Weeks= weeks,
        };

        return Result.Success<GetPerformanceAnalysisQuery>("Query successful.");
    }

    public class PerformanceAnalysis
    {
        public List<PerformanceWeek> Weeks {  get; set; } = [];
    } 
    public class PerformanceWeek
    {
        public float? StudentScore { get; set; }
        public float? AverageScore { get; set; }
    }

    private async Task<float?> GetStudentScore(string studentId,DateTime startWeek)
    {
        Student? student = await _userManager.Users.Include(s => s.AssignedTasks)
                                .FirstOrDefaultAsync(s => s.Id == studentId, CancellationToken.None);

        DateTime oneWeekAhead = startWeek.Add(TimeSpan.FromDays(7));

        var gradedTasksForTheWeek = await _context.Tasks
       .Where(t => t.CreatedDate <= oneWeekAhead &&
                  t.CreatedDate >= startWeek)
       .Join(
           inner: _context.SubmissionDetails.Where(sd => sd.StudentId == studentId),
           outerKeySelector: mt => mt.Id,
           innerKeySelector: sd => sd.TaskId,
           resultSelector: (mt, sd) => new { ModuleTask = mt, SubmissionDetails = sd })
       .Where(mt => mt.SubmissionDetails.TaskStatus == ModuleTaskStatus.Graded)
       .ToListAsync();

        float totalScore = 0f;
        int taskCount = 0;


        foreach (var task in gradedTasksForTheWeek.Where(task => task.SubmissionDetails.Grade.HasValue))
        {
            totalScore += task.SubmissionDetails.Grade.Value;
            taskCount++;
        }

        if (taskCount == 0)
        {
            return null; // No tasks with scores
        }

        return totalScore / taskCount; // Calculate average
    }

    private async Task<int> GetCompletedTasks(string studentId)
    {
        //get the tasks for the specific student
        Student? student = await _userManager.Users.Include(s => s.AssignedTasks)
                                 .FirstOrDefaultAsync(s => s.Id == studentId, CancellationToken.None);

        int completedTasks = await _context.Tasks
       .Join(
           inner: _context.SubmissionDetails.Where(sd => sd.StudentId == studentId),
           outerKeySelector: mt => mt.Id,
           innerKeySelector: sd => sd.TaskId,
           resultSelector: (mt, sd) => new { ModuleTask = mt, SubmissionDetails = sd })
       .Where(mt => mt.SubmissionDetails.TaskStatus == ModuleTaskStatus.Submitted)
       .CountAsync();

        return completedTasks;
    }


}