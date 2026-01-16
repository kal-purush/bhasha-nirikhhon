using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace HairSalon.Web.Pages.Admins.DashBoard
{
    public class DashBoardModel : PageModel
    {
        private readonly HairSalon.Infrastructure.HairSalonDbContext _context;

        public DashBoardModel(HairSalon.Infrastructure.HairSalonDbContext context)
        {
            _context = context;
        }

        public int TotalAppointments { get; set; }
        public int TotalCustomers { get; set; }
        public int TotalEmployees { get; set; }
        public int TotalServices { get; set; }
        public List<int> AppointmentsPerDay { get; set; } = new List<int>();

        [BindProperty(SupportsGet = true)]
        public string TimeRange { get; set; } = "LastWeek";

        [BindProperty(SupportsGet = true)]
        public int PageNumber { get; set; } = 1;

        [BindProperty(SupportsGet = true)]
        public string SearchTerm { get; set; }

        [BindProperty(SupportsGet = true)]
        public string SortField { get; set; } = "AppointmentDate";

        [BindProperty(SupportsGet = true)]
        public string SortDirection { get; set; } = "asc";

        public int TotalPages { get; set; }

        private const int PageSize = 10;

        public async Task OnGetAsync()
        {
            TotalAppointments = await _context.Appointments.CountAsync();
            TotalCustomers = await _context.Customers.CountAsync();
            TotalEmployees = await _context.Employees.CountAsync();
            TotalServices = await _context.Services.CountAsync();

            DateTime startDate = DateTime.Now;
            switch (TimeRange)
            {
                case "LastWeek":
                    startDate = DateTime.Now.AddDays(-7);
                    break;
                case "LastMonth":
                    startDate = DateTime.Now.AddMonths(-1);
                    break;
                case "LastYear":
                    startDate = DateTime.Now.AddYears(-1);
                    break;
            }

            var appointmentsQuery = _context.Appointments
                .Where(a => a.AppointmentDate.ToDateTime(TimeOnly.MinValue) >= startDate);

            if (!string.IsNullOrEmpty(SearchTerm))
            {
                appointmentsQuery = appointmentsQuery.Where(a => a.Customer.Name.Contains(SearchTerm) || a.Stylist.Name.Contains(SearchTerm));
            }

            switch (SortField)
            {
                case "CustomerName":
                    appointmentsQuery = SortDirection == "asc" ? appointmentsQuery.OrderBy(a => a.Customer.Name) : appointmentsQuery.OrderByDescending(a => a.Customer.Name);
                    break;
                case "StylistName":
                    appointmentsQuery = SortDirection == "asc" ? appointmentsQuery.OrderBy(a => a.Stylist.Name) : appointmentsQuery.OrderByDescending(a => a.Stylist.Name);
                    break;
                default:
                    appointmentsQuery = SortDirection == "asc" ? appointmentsQuery.OrderBy(a => a.AppointmentDate) : appointmentsQuery.OrderByDescending(a => a.AppointmentDate);
                    break;
            }

            var groupedAppointmentsQuery = appointmentsQuery
                .GroupBy(a => a.AppointmentDate)
                .Select(g => g.Count());

            TotalPages = (int)Math.Ceiling(await groupedAppointmentsQuery.CountAsync() / (double)PageSize);

            AppointmentsPerDay = await groupedAppointmentsQuery
                .Skip((PageNumber - 1) * PageSize)
                .Take(PageSize)
                .ToListAsync();
        }
    }
}