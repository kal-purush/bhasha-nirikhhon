using Application.Common.Models;
using MediatR;
using Domain.Entities;
using Microsoft.AspNetCore.Identity;
using Application.Interfaces;
using Microsoft.EntityFrameworkCore;

namespace Application.Programmes.Queries;

public class GetAllProgrammesQuery : IRequest<Result>
{

}

public class GetAllProgrammesQueryHandler(IApplicationDbContext context) : IRequestHandler<GetAllProgrammesQuery, Result>
{
    private readonly IApplicationDbContext _context = context;

    public async Task<Result> Handle(GetAllProgrammesQuery request, CancellationToken cancellationToken)
    {
        List<Programme>? Programmes = await _context.Programmes.ToListAsync();

        if (Programmes == null)
        {
            return Result.Failure<GetAllProgrammesQuery>("No Programmes");
        }

        // Return the user or null if not found
        return Result.Success<GetAllProgrammesQuery>("Programmes retrieved successfully.", Programmes);
    }
}