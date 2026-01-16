using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.AspNetCore.Mvc.Rendering;
using HairSalon.Core.Entities;
using HairSalon.Infrastructure;
using HairSalon.Web.Pages.Endpoints;
using Newtonsoft.Json;
using System.ComponentModel.DataAnnotations.Schema;
using System.ComponentModel.DataAnnotations;

namespace HairSalon.Web.Pages.Appointments
{
    public class CreateServiceModel : PageModel
    {
        private readonly HttpClient _httpClient;
        private readonly IHttpContextAccessor _httpContextAccessor;

        public CreateServiceModel(
            HttpClient httpClient,
            IHttpContextAccessor httpContextAccessor)
        {
            _httpClient = httpClient;
            _httpContextAccessor = httpContextAccessor;
        }

        public async Task<IActionResult> OnGet()
        {
            await Populate();
            return Page();
        }
        public List<Core.Entities.Service> services { get; set; } = default!;
        public List<ServiceView> serviceViews { get; set; } = default!;
        [BindProperty]
        public DateTime date { get; set; } = default!;
        [BindProperty]
        [Range(typeof(TimeOnly), "07:00", "20:00", ErrorMessage = "Must be between 07:00 and 20:00.")]
        public TimeOnly startTime { get; set; } = default!;
        [BindProperty]
        [Range(typeof(TimeOnly), "00:00", "21:00", ErrorMessage = "Choose start time again to make endtime before 21:00.")]
        public TimeOnly endTime { get; set; } = default!;
        [BindProperty]
        public List<int> selectedServiceIds { get; set; } = new List<int>();

        // For more information, see https://aka.ms/RazorPagesCRUD.
        public async Task<IActionResult> OnPostAsync()
        {
            if (!ModelState.IsValid)
            {
                await Populate();
                return Page();
            }
            date = new DateTime(date.Year, date.Month, date.Day, startTime.Hour, startTime.Minute, startTime.Second);
            if (date <= DateTime.Now.AddHours(1).AddMinutes(30))
            {
                await Populate();
                ModelState.AddModelError(string.Empty, "The selected date must be at least around 2 hours after the current time.");
                return Page();
            }
            if (selectedServiceIds.Count<=0)
            {
                await Populate();
                ModelState.AddModelError(string.Empty, "Must select service(s).");
                return Page();
            }

            TempData["AppointmentDatr"] = JsonConvert.SerializeObject(date);
            TempData["AppointmentStartTime"] = JsonConvert.SerializeObject(startTime);
            TempData["AppointmentEndTime"] = JsonConvert.SerializeObject(endTime);
            TempData["selectedServiceIds"] = JsonConvert.SerializeObject(selectedServiceIds);

            return RedirectToPage("./Create");
        }

        private async Task Populate()
        {
            date = DateTime.Now;
            var servicesResponse = await _httpClient.GetAsync(ApplicationEndpoint.GetHairServiceEndpoint);
            services = await servicesResponse.Content.ReadFromJsonAsync<List<Core.Entities.Service>>();
            serviceViews = services.Select(s => new ServiceView
            {
                Id = s.Id,
                Name = s.Name + " (" + s.Duration + ")",
                Duration = formatDurationToNumber(s.Duration) ,
            }).ToList();
        }

        private int formatDurationToNumber(string duration)
        {
            int spaceIndex = duration.IndexOf(" ");

            if (spaceIndex > -1)
            {
                string numericPart = duration.Substring(0, spaceIndex);

                if (int.TryParse(numericPart, out int minutes))
                {
                    return minutes;
                }
            }
            return 0;
        }
        public class ServiceView {
            public int Id { get; set; }

            [StringLength(100)]
            public string Name { get; set; } = null!;

            public int Duration { get; set; }
        }
    }
}