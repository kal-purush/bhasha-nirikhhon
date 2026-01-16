using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;

namespace PopApis.Controllers
{
    public class CommonController : Controller
    {
        public virtual ActionResult ViewWithSession(string viewName = null, object model = null)
        {
            var role = HttpContext.Session.GetString("UserRole");
            if (string.IsNullOrWhiteSpace(role) ||
                !string.Equals(role, "Admin", StringComparison.OrdinalIgnoreCase))
            {
                return RedirectToAction("Index", "Home");
            }
            return View(viewName, model);
        }
    }
}