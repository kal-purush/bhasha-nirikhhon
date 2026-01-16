using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;

namespace GPT_Poker.Controllers;

public class BaseController : Controller
{
    public override void OnActionExecuting(ActionExecutingContext context)
    {
        if (context.RouteData.Values["Action"] as string == "Logout") return;
        if (HttpContext.Session.TryGetValue("user", out _))
        {
            context.Result = new RedirectToRouteResult(new RouteValueDictionary{{ "controller", "Home" },  
                { "action", "Index" }  
  
            });  
        }
        
        base.OnActionExecuting(context);
    }
}