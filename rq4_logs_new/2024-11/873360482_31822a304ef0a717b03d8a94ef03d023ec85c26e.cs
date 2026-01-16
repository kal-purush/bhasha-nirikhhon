using HairSalon.Web.Pages.Endpoints;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace HairSalon.Web.Pages.Admins.DashBoard
{
    public class CreateModel : PageModel
    {
        private readonly HttpClient _httpClient;
        private readonly IHttpContextAccessor _httpContextAccessor;

        [BindProperty]
        public AddServiceModel Service { get; set; }

        public CreateModel(HttpClient httpClient, IHttpContextAccessor httpContextAccessor)
        {
            _httpClient = httpClient;
            _httpContextAccessor = httpContextAccessor;

            var token = _httpContextAccessor.HttpContext?.Session.GetString("CustomerToken") ??
                        _httpContextAccessor.HttpContext?.Session.GetString("EmpToken");
            if (!string.IsNullOrEmpty(token))
            {
                _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
            }
        }
        public void OnGet()
        {
        }

        public async Task<IActionResult> OnPostAsync()
        {
            var serviceJson = JsonSerializer.Serialize(Service);
            var content = new StringContent(serviceJson, Encoding.UTF8, "application/json");
            var response = await _httpClient.PostAsync(ApplicationEndpoint.CreateHairServiceEndpoint, content);

            if (!response.IsSuccessStatusCode)
            {
                var responseContent = await response.Content.ReadAsStringAsync();
                Console.WriteLine($"Response Status Code: {response.StatusCode}");
                Console.WriteLine($"Response Content: {responseContent}");

                ModelState.AddModelError(string.Empty, "Unable to create service. Please try again.");
                return Page();
            }

            return RedirectToPage("/Admins/DashBoard/Service");
        }
    }

    public class AddServiceModel
    {
        public string Name { get; set; }
        public string Description { get; set; }
        public string EstimatedDuration { get; set; }
        public decimal Price { get; set; }
    }
}