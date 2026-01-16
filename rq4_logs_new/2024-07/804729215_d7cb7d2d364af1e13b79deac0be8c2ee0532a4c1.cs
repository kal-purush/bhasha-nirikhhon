using Application.Abstractions.Data;
using Contract.Abstractions.Messages;
using Contract.Abstractions.Shared.Results;
using Contract.Services.MonthEmployeeSalary;
using Contract.Services.SalaryHistory.ShareDtos;
using Domain.Entities;

namespace Application.UserCases.Commands.MonthlyEmployeeSalaries;

public sealed class CreateMonthEmployeeSalaryCommandHandler(
     IUserRepository _userRepository,
     IAttendanceRepository _attendanceRepository,
     IMonthlyEmployeeSalaryRepository _monthlyEmployeeSalaryRepository,
     IUnitOfWork _unitOfWork) : ICommandHandler<CreateMonthEmployeeSalaryCommand>
{
    public async Task<Result.Success> Handle(CreateMonthEmployeeSalaryCommand request, CancellationToken cancellationToken)
    {
        var month = request.month;
        var year = request.year;

        // Lấy tất cả dữ liệu chấm công và sản phẩm của nhân viên
        var users = await _userRepository.GetAttendanceAndEmployeeProductAllUser(month, year);
        var monthlyEmployeeSalaries = new List<MonthlyEmployeeSalary>();
        foreach (var user in users)
        {
            var attendances = await _attendanceRepository.GetAttendanceByMonthAndUserIdAsync(month, year, user.Id);

            var employeeProducts = user.EmployeeProducts.Where(ep => ep.Date.Month == month && ep.Date.Year == year).ToList();

            decimal TotalSalary = 0;
            TotalSalary += CalculatorProductSalary(attendances, employeeProducts);

            var SalaryMonthByUser = CalculateTotalMonthlySalary(attendances, user.SalaryHistories, user.Id, month, year);

            TotalSalary += SalaryMonthByUser;

            // Tạo bản ghi lương tháng của nhân viên
            var monthlyEmployeeSalary = MonthlyEmployeeSalary.Create(new CreateMonthlyEmployeeSalaryRequest
            (
                UserId: user.Id,
                Month: month,
                Year: year,
                Salary: TotalSalary
            ));
            monthlyEmployeeSalaries.Add(monthlyEmployeeSalary);
            decimal currentBalance = user.AccountBalance ?? 0;

            user.UpdateAccountBalance(currentBalance + TotalSalary);
        }
        _monthlyEmployeeSalaryRepository.AddRange(monthlyEmployeeSalaries);
        _userRepository.UpdateRange(users);
        await _unitOfWork.SaveChangesAsync();
        return Result.Success.Create(); // Trả về kết quả thành công
    }

    // Tính lương theo sản phẩm.
    private decimal CalculatorProductSalary(List<Attendance> attendances, List<EmployeeProduct> employeeProducts)
    {
        var productSalaries = employeeProducts
            .Join(attendances,
                  ep => new { ep.UserId, ep.SlotId, ep.Date },
                  a => new { a.UserId, a.SlotId, a.Date },
                  (ep, a) => new { ep, a })
            .Where(joined => joined.a.IsSalaryByProduct == true)
            .SelectMany(joined => joined.ep.Product.ProductPhaseSalaries,
                        (joined, pps) => pps.SalaryPerProduct * joined.ep.Quantity)
            .Sum();

        return productSalaries;
    }

    // Lấy ra list salaryHistory cần thiết để tính lương
    private List<SalaryHistory> GetRelevantSalaryHistories(List<SalaryHistory> salaryHistories, int month, int year)
    {
        var relevantHistories = salaryHistories
            .Where(sh => sh.StartDate >= new DateOnly(year, month, 1) && sh.StartDate <= new DateOnly(year, month, DateTime.DaysInMonth(year, month)))
            .OrderBy(sh => sh.StartDate)
            .ToList();

        // Lấy lịch sử lương gần nhất trước tháng hiện tại nếu lịch sử lương tháng hiện tại không bắt đầu từ ngày 1
        var firstHistoryInMonth = relevantHistories.FirstOrDefault(sh => sh.StartDate.Month == month && sh.StartDate.Year == year);
        if (firstHistoryInMonth != null && firstHistoryInMonth.StartDate > new DateOnly(year, month, 1))
        {
            var closestHistory = salaryHistories
                .Where(sh => sh.StartDate < new DateOnly(year, month, 1))
                .OrderByDescending(sh => sh.StartDate)
                .FirstOrDefault();

            if (closestHistory != null)
            {
                relevantHistories.Insert(0, closestHistory);
            }
        }
        else if (firstHistoryInMonth == null) // Nếu không có lịch sử lương trong tháng hiện tại
        {
            var closestHistory = salaryHistories
                .Where(sh => sh.StartDate < new DateOnly(year, month, 1))
                .OrderByDescending(sh => sh.StartDate)
                .FirstOrDefault();

            if (closestHistory != null)
            {
                relevantHistories.Insert(0, closestHistory);
            }
        }

        return relevantHistories;
    }

    // Tính lương theo ngày hoặc giờ tăng ca   
    private decimal CalculateMonthlySalary(List<Attendance> attendances, List<SalaryHistory> salaryHistories, string userId, int month, int year, SalaryType salaryType)
    {
        // Lọc lịch sử lương của nhân viên với SalaryType cụ thể
        var userSalaryHistories = salaryHistories
            .Where(sh => sh.UserId == userId && sh.SalaryType == salaryType)
            .ToList();

        var relevantHistories = GetRelevantSalaryHistories(userSalaryHistories, month, year);

        if (!relevantHistories.Any())
            return 0;

        decimal totalSalary = 0;

        // Tính lương theo ngày
        for (int i = 0; i < relevantHistories.Count; i++)
        {
            var salaryHistory = relevantHistories[i];

            var nextSalaryHistoryStartDate = i + 1 < relevantHistories.Count ? relevantHistories[i + 1].StartDate : new DateOnly(year, month, DateTime.DaysInMonth(year, month));

            var applicableAttendances = attendances
                .Where(a => a.UserId == userId
                            && a.Date >= salaryHistory.StartDate
                            && a.Date < nextSalaryHistoryStartDate
                            && a.IsAttendance
                            && !a.IsSalaryByProduct)
                .ToList();

            if (salaryType == SalaryType.SALARY_BY_DAY)
            {
                var workingDays = applicableAttendances.Count(a => a.SlotId == 1 || a.SlotId == 2);
                totalSalary += workingDays * salaryHistory.Salary;
            }
            else if (salaryType == SalaryType.SALARY_OVER_TIME)
            {
                var overTimeHours = applicableAttendances
                    .Where(a => a.SlotId == 1 || a.SlotId == 2 || a.SlotId == 3)
                    .Sum(a => a.HourOverTime);
                totalSalary += Convert.ToDecimal(overTimeHours) * salaryHistory.Salary;
            }
        }
        // calculate salary for the last day of month
        var lastDayOfMonth = new DateOnly(year, month, DateTime.DaysInMonth(year, month));
        var lastDayAttendances = attendances
            .Where(a => a.UserId == userId
                    && a.Date == lastDayOfMonth
                    && a.IsAttendance
                    && !a.IsSalaryByProduct)
            .ToList();

        if (lastDayAttendances.Any())
        {
            if (salaryType == SalaryType.SALARY_BY_DAY)
            {
                var workingDays = lastDayAttendances.Count(a => a.SlotId == 1 || a.SlotId == 2);
                totalSalary += workingDays * relevantHistories.Last().Salary;
            }
            else if (salaryType == SalaryType.SALARY_OVER_TIME)
            {
                var overTimeHours = lastDayAttendances
                    .Where(a => a.SlotId == 1 || a.SlotId == 2 || a.SlotId == 3)
                    .Sum(a => a.HourOverTime);
                totalSalary += Convert.ToDecimal(overTimeHours) * relevantHistories.Last().Salary;
            }
        }

        return totalSalary;
    }

    private decimal CalculateTotalMonthlySalary(List<Attendance> attendances, List<SalaryHistory> salaryHistories, string userId, int month, int year)
    {
        var workingDaySalary = CalculateMonthlySalary(attendances, salaryHistories, userId, month, year, SalaryType.SALARY_BY_DAY);
        var overTimeSalary = CalculateMonthlySalary(attendances, salaryHistories, userId, month, year, SalaryType.SALARY_OVER_TIME);
        return workingDaySalary + overTimeSalary;
    }

}