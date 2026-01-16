
using Microsoft.EntityFrameworkCore;
using WolfDen.Application.DTOs.LeaveManagement;
using WolfDen.Domain.Enums;
using WolfDen.Infrastructure.Data;

namespace WolfDen.Application.Method.LeaveManagement
{
    public class CalculateLeaveDays(WolfDenContext context)
    {
        private readonly WolfDenContext _context = context;

        public async Task<LeaveDaysResultDto> LeaveDays(DateOnly fromDate, DateOnly toDate, bool sandwich)
        {
            LeaveDaysResultDto leaveDaysResultDto = new LeaveDaysResultDto();
            if (sandwich)
            {
                // Total number of days between the two dates, inclusive
                leaveDaysResultDto.DaysCount = (toDate.DayNumber - fromDate.DayNumber) + 1;
                for (DateOnly date = fromDate; date <= toDate; date = date.AddDays(1))
                {
                    leaveDaysResultDto.ValidDate.Add(date);
                }
            }
            else
            {
                List<DateOnly> holidayDates = await _context.Holiday
                   .Where(x => x.Type == AttendanceStatus.NormalHoliday)
                   .Select(x => x.Date)
                   .ToListAsync();

                for (DateOnly date = fromDate; date <= toDate; date = date.AddDays(1))
                {
                    DayOfWeek dayOfWeek = date.DayOfWeek;
                    if (dayOfWeek != DayOfWeek.Saturday &&
                        dayOfWeek != DayOfWeek.Sunday &&
                        !holidayDates.Contains(date))
                    {
                        leaveDaysResultDto.ValidDate.Add(date);
                    }
                }

                // Count the valid dates
                leaveDaysResultDto.DaysCount = leaveDaysResultDto.ValidDate.Count;
            }
            return leaveDaysResultDto;
        }
    }
}