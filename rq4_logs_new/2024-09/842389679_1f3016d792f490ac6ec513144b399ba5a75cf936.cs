using Application.Common.Models;
using Application.Interfaces;
using Domain.Entities;
using MediatR;
using Microsoft.EntityFrameworkCore;

namespace Application.Notifications.Queries;

public class GetAllNotificationsQuery : IRequest<Result>
{
}

public class GetAllNotificationsQueryHandler(IApplicationDbContext context) : IRequestHandler<GetAllNotificationsQuery, Result>
{
    private readonly IApplicationDbContext _context = context;

    public async Task<Result> Handle(GetAllNotificationsQuery request, CancellationToken cancellationToken)
    {
        List<Notification>? notifications = await _context.Notifications.ToListAsync();

        if (notifications == null)
        {
            return Result.Failure<GetAllNotificationsQuery>("No notifications");
        }

        // Return the user or null if not found
        return Result.Success<GetAllNotificationsQuery>("Notifications retrieved successfully.", notifications);
    }
}