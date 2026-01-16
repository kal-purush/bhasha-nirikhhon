using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace PizzaClient.Razor.Pages;

public class OrderModel : PageModel
{
    private readonly ILogger<OrderModel> _logger;
    private readonly IHttpClientFactory _clientFactory;

    public OrderModel(ILogger<OrderModel> logger, IHttpClientFactory clientFactory)
    {
        _logger = logger;
        _clientFactory = clientFactory;
    }

    public async Task<IActionResult> OnGetAsync()
    {
        // _logger.LogInformation(TempData["dough"].ToString());
        return Page();
    }

    // public async Task<IActionResult> OnPostAsync()
    // {
        // var jsonOrder = JsonSerializer.Serialize<Order>(order);
        // StringContent newOrder = new StringContent(jsonOrder, Encoding.UTF8, "application/json");
        // var response = await client.PostAsync("order", newOrder);

        // if (response.IsSuccessStatusCode)
        // {
        //     order.GetOrderPrice();
        //     AnsiConsole.Markup("[blue]Thank you for visiting our store![/]");
        // }
        // else
        // {
        //     AnsiConsole.Markup("[crimson]We could not place your order :([/]");
        // }
    // }
}