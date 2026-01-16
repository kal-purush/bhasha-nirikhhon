using MediatR;
using Microsoft.EntityFrameworkCore;
using WolfDen.Application.DTOs.Attendence;
using WolfDen.Domain.Entity;
using WolfDen.Infrastructure.Data;

namespace WolfDen.Application.Requests.Queries.Attendence.AllEmployeesMonthlyReport
{
    public class AllEmployeesMonthlyReportQueryHandler : IRequestHandler<AllEmployeesMonthlyReportQuery, MonthlyReportAndPageCountDTO>
    {
        private readonly WolfDenContext _context;
        public AllEmployeesMonthlyReportQueryHandler(WolfDenContext context)
        {
            _context = context;
        }
        public async Task<MonthlyReportAndPageCountDTO> Handle(AllEmployeesMonthlyReportQuery request, CancellationToken cancellationToken)
        {
            MonthlyReportAndPageCountDTO monthlyReportAndPageCountDTO = new MonthlyReportAndPageCountDTO();
            List<AllEmployeesMonthlyReportDTO> allEmployeesReport = new List<AllEmployeesMonthlyReportDTO>();
            List<LOP> LOPReport = await _context.LOP.Where(x => x.AttendanceClosedDate.Month == request.Month).Include(x=>x.Employee).ToListAsync(cancellationToken);
            if (LOPReport is not null)
            {
                foreach(var lop in LOPReport)
                {
                    AllEmployeesMonthlyReportDTO report = new AllEmployeesMonthlyReportDTO();
                    report.EmployeeId = lop.EmployeeId;
                    report.EmployeeName = lop.Employee.FirstName + " " + lop.Employee.LastName;
                    report.IncompleteShiftDays = lop.IncompleteShiftDays;
                    report.NofIncompleteShiftDays = lop.NoOfIncompleteShiftDays;
                    report.AbsentDays = lop.LOPDays;
                    report.NoOfAbsentDays = lop.LOPDaysCount;
                    allEmployeesReport.Add(report);
                }
            }
            int totalPage = allEmployeesReport.Count();
            int noOfPage =(int)(Math.Ceiling((decimal)totalPage / request.PageSize));
            List<AllEmployeesMonthlyReportDTO> allEmployeesReports=allEmployeesReport.Skip(request.PageNumber-1*request.PageSize).Take(request.PageSize).ToList();
            monthlyReportAndPageCountDTO.AllEmployeesMonthlyReports = allEmployeesReports;
            monthlyReportAndPageCountDTO.PageCount = noOfPage;
            return monthlyReportAndPageCountDTO;
        }
    }
}