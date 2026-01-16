using Microsoft.EntityFrameworkCore;
using WolfDen.Domain.Enums;
using WolfDen.Infrastructure.Data;

namespace WolfDen.Application.Methods.LeaveManagement
{
    public class CalculateLeaveDays(WolfDenContext context)
    {
        private readonly WolfDenContext _context = context;

        public async Task<int> LeaveDays(DateOnly fromDate, DateOnly toDate, bool sandwich)
        {
            if (sandwich)
            {
                // Total number of days between the two dates, inclusive
                return (toDate.DayNumber - fromDate.DayNumber) + 1;
            }
            else
            {
                // Calculate weekdays only
                int daysCount = 0;
                List<DateOnly> holidayDates = await _context.Holiday.Where(x => x.Type == AttendanceStatus.NormalHoliday).Select(x => x.Date).ToListAsync();
                for (DateOnly date = fromDate; date <= toDate; date = date.AddDays(1))
                {
                    DayOfWeek dayOfWeek = date.DayOfWeek;
                    if (dayOfWeek != DayOfWeek.Saturday && dayOfWeek != DayOfWeek.Sunday && !holidayDates.Contains(date) )
                    {
                        daysCount++;
                    }
                }
                return daysCount;
            }
        }
    }
}