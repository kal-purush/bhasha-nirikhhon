using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace TestSelect2.ViewComponents
{
    public class TestViewComponent: ViewComponent
    {
        public TestViewComponent()
        {
        }

        public async Task<IViewComponentResult> InvokeAsync(
        int maxPriority, bool isDone)
        {
            return View();
        }

    }
}