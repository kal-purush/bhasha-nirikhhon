using Application.Common.Models;
using Application.Interfaces;
using MediatR;
using Microsoft.EntityFrameworkCore;


namespace Application.Students.Queries;

public class GetStudentApplicationHistoryQuery : IRequest<Result>
{
    public string StudentId { get; set; }
}

public class GetStudentApplicationHistoryQueryHandler : IRequestHandler<GetStudentApplicationHistoryQuery, Result>
{
    private readonly IApplicationDbContext _context;

    public GetStudentApplicationHistoryQueryHandler(IApplicationDbContext context)
    {
        _context = context;
    }

    public async Task<Result> Handle(GetStudentApplicationHistoryQuery request, CancellationToken cancellationToken)
    {
        // Retrieve the student's transaction history from the database
        var applicationHistory = await _context.Transactions
            .Where(t => t.StudentId == request.StudentId)
            .Select(t => new ApplicationHistoryDto
            {
                ApplicationDate = t.CreatedAt,
                Amount = t.Amount,
                TransactionStatus = t.TransactionStatus,
                ApplicationType = t.ApplicationType.ToString(), // Convert enum to string
                PreferredProgramme = t.PreferredProgramme
            })
            .ToListAsync(cancellationToken);

        if (!applicationHistory.Any())
        {
            return Result.Failure<List<ApplicationHistoryDto>>("No application history found.");
        }

        return Result.Success(applicationHistory);
    }
}

public class ApplicationHistoryDto
{
    public DateTime ApplicationDate { get; set; }
    public decimal Amount { get; set; }
    public string TransactionStatus { get; set; }
    public string ApplicationType { get; set; }
    public string PreferredProgramme { get; set; }
}