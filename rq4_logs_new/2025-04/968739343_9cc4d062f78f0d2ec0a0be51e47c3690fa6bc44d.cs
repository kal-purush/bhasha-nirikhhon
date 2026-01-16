using System.Diagnostics;
using Microsoft.AspNetCore.Mvc;
using inTouch_demo.Models;
using System.DirectoryServices.AccountManagement;
using Microsoft.AspNetCore.Authentication;

namespace inTouch_demo.Controllers;

public class HomeController : Controller
{
    private readonly ILogger<HomeController> _logger;
    private readonly IHttpClientFactory _httpClientFactory;

    public HomeController(ILogger<HomeController> logger, IHttpClientFactory httpClientFactory)
    {
        _logger = logger;
        _httpClientFactory = httpClientFactory;
    }

    public IActionResult Index()
    {
        var model = new ResetRequest();
        ViewBag.Users = GetLocalUsers();
        return View(model);
    }

    [HttpPost]
    public async Task<IActionResult> Index(ResetRequest model)
    {
        ViewBag.Users = GetLocalUsers();

        if (string.IsNullOrWhiteSpace(model.Username) || string.IsNullOrWhiteSpace(model.NewPassword))
        {
            model.Result = "Username and Password are required.";
            return View(model);
        }

        try
        {
            var client = _httpClientFactory.CreateClient();
            var response = await client.PostAsJsonAsync("http://localhost:5000/reset-password", model);
            model.Result = await response.Content.ReadAsStringAsync();
        }
        catch (Exception ex)
        {
            model.Result = $"Error calling API: {ex.Message}";
        }

        return View(model);
    }

    private List<string> GetLocalUsers()
    {
        var users = new List<string>();
        using (var ctx = new PrincipalContext(ContextType.Machine))
        {
            using (var searcher = new PrincipalSearcher(new UserPrincipal(ctx)))
            {
                foreach (var result in searcher.FindAll())
                {
                    if (result is UserPrincipal user)
                    {
                        users.Add(user.SamAccountName);
                    }
                }
            }
        }
        return users;
    }

    public IActionResult Privacy()
    {
        return View();
    }

    [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
    public IActionResult Error()
    {
        return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
    }
}