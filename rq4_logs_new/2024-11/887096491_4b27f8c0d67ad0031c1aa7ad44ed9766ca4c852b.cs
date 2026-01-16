using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MediatR;
using Microsoft.EntityFrameworkCore;
using WolfDen.Application.Requests.DTOs.Attendence;
using WolfDen.Infrastructure.Data;

namespace WolfDen.Application.Requests.Queries.Attendence.DailyStatus
{
    public class DailyStatusQueryHandler : IRequestHandler<DailyStatusQuery, List<DailyStatusDTO>>
    {
        private readonly WolfDenContext _context;

        public DailyStatusQueryHandler(WolfDenContext context)
        {
            _context = context;
        }


        public async Task<List<DailyStatusDTO>> Handle(DailyStatusQuery request, CancellationToken cancellationToken)
        {
            if (request.Year <= 0 || request.Month <= 0 || request.Month > 12)
            {
                throw new ArgumentException("invalid year and month");
            }

            var monthStart = new DateOnly(request.Year, request.Month, 1);
            var monthEnd = monthStart.AddMonths(1).AddDays(-1);

            var attendanceData = await _context.Status
                .Where(s => s.EmployeeId == request.EmployeeId && s.Date >= monthStart && s.Date <= monthEnd)
                .Include(s => s.StatusType)
                .Select(s => new DailyStatusDTO
                {
                    Date=s.Date,
                    Status=s.StatusType.StatusName
                })
                .ToListAsync();

            return attendanceData;
        }
    }
}