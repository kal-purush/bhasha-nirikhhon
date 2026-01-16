using System.Net;
using CraftHouse.Web.Data;
using CraftHouse.Web.Entities;
using CraftHouse.Web.Services;
using Microsoft.AspNetCore.Http.Extensions;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace CraftHouse.Web.Pages.Admin;

public class Users : PageModel
{
    private readonly AppDbContext _context;
    private readonly IAuthService _authService;
    private readonly ILogger<Users> _logger;

    public Users(AppDbContext context, IAuthService authService, ILogger<Users> logger)
    {
        _context = context;
        _authService = authService;
        _logger = logger;
    }

    public List<User> AppUsers { get; set; } = null!;

    public IActionResult OnGet()
    {
        var user = _authService.GetLoggedInUser();
        if (user == null)
        {
            _logger.LogInformation("Location was: {@location}", HttpContext.Request.Path.Value);
            return Redirect("/login?RedirectUrl=" + HttpContext.Request.Path.Value);
        }

        AppUsers = _context.Users.ToList();

        return Page();
    }
}