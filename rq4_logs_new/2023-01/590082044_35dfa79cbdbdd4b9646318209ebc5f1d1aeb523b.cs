using CraftHouse.Web.Entities;
using CraftHouse.Web.Services;
using Microsoft.AspNetCore.Mvc;

namespace CraftHouse.Web.ViewComponents;

public class MenuViewComponent : ViewComponent
{
    private readonly IAuthService _authService;

    public MenuViewComponent(IAuthService authService)
    {
        _authService = authService;
    }
    
    public IViewComponentResult Invoke()
    {
        var user = _authService.GetLoggedInUser();
        var model = new MenuViewComponentModel()
        {
            User = user
        };
        return View("Default", model);
    }
}

public class MenuViewComponentModel
{
    public User? User { get; set; }
}