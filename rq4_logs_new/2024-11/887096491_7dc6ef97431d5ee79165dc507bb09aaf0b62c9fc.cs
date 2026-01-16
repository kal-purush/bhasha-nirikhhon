using MediatR;
using Microsoft.EntityFrameworkCore;
using WolfDen.Domain.Entity;
using WolfDen.Infrastructure.Data;

namespace WolfDen.Application.Requests.Commands.Attendence.UpdateAllNotification
{
    public class UpdateAllNotificationCommandHandler(WolfDenContext context) : IRequestHandler<UpdateAllNotificationCommand,bool>
    {
        private readonly WolfDenContext _context = context;
        public async Task<bool> Handle(UpdateAllNotificationCommand request, CancellationToken cancellationToken)
        {
            List<Notification> notifications = await _context.Notifications
                .Where(x => x.EmployeeId == request.EmployeeId && x.IsRead==false).ToListAsync(cancellationToken);

            foreach (Notification notification in notifications)
            {
                notification.MarkAsRead();
            }
            return await _context.SaveChangesAsync(cancellationToken)>0;    
        }
    }
}