using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Newtonsoft.Json;

namespace TripEnjoy.Presentation.Razor.Pages.Booking
{
    public class BookingDetailsModel : PageModel
    {
        private readonly IHttpClientFactory httpClientFactory;
        public BookingDetailsModel(IHttpClientFactory httpClientFactory)
        {
            this.httpClientFactory = httpClientFactory;
        }

        [BindProperty(SupportsGet = true)]
        public ViewModels.Booking Booking { get; set; }
        public async Task<IActionResult> OnGetAsync(int id)
        {
            var client = httpClientFactory.CreateClient("DefaultClient");
            client.Timeout = TimeSpan.FromMinutes(2);
            // booking details
            var bookingRequest = new HttpRequestMessage(HttpMethod.Get, $"https://localhost:7126/api/Booking/{id}");
            var response = await client.SendAsync(bookingRequest);
            if (response.IsSuccessStatusCode)
            {
                var bookingContent = await response.Content.ReadAsStringAsync();
                Booking = JsonConvert.DeserializeObject<ViewModels.Booking>(bookingContent);
            }
            return Page();
        }
    }
}