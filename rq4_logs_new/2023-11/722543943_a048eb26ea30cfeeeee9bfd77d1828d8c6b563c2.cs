using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using TodosWebApp.Web.ViewModels;

namespace TodosWebApp.Web.Views.Shared.ViewComponents
{
    public class FormViewComponent : ViewComponent
    {
        public async Task<IViewComponentResult> InvokeAsync(string action, TodoViewModel formModel)
        {
            ViewBag.title = action;
            // TodoViewModel todoViewModel = null;
            return View(formModel);
        }
    }
}