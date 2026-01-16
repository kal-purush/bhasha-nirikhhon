using HairSalon.Core.Entities;
using HairSalon.Web.Pages.Endpoints;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace HairSalon.Web.Pages.Appointments
{
    public class StylistDetailModel : PageModel
    {
        private readonly HttpClient _httpClient;
        private readonly IHttpContextAccessor _httpContextAccessor;

        public StylistDetailModel(
            HttpClient httpClient,
            IHttpContextAccessor httpContextAccessor)
        {
            _httpClient = httpClient;
            _httpContextAccessor = httpContextAccessor;
        }

        public Employee Employee { get; set; } = default!;
        public int appointmentId { get; set; } = default!;

        public async Task<IActionResult> OnGetAsync(int? id, int? stylistId)
        {
            if (id == null || stylistId == null)
            {
                return NotFound();
            }

            appointmentId = id??0;

            var employee = await _httpClient.GetAsync(ApplicationEndpoint.EmployeeGetByIdEndPoint + stylistId.ToString());
            if (employee == null)
            {
                return NotFound();
            }
            else
            {
                Employee = await employee.Content.ReadFromJsonAsync<Employee>();
            }
            return Page();
        }
    }
}