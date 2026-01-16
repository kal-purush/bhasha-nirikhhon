using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNetCore.Mvc;

namespace CoolBytes.WebAPI.Extensions
{
    public static class ControllerExtensions
    {
        public static IActionResult OkOrNotFound(this Controller controller, object model)
        {
            if (model != null)
                return controller.Ok(model);

            return controller.NotFound();
        }

        public static IActionResult OkOrNotFound<T>(this Controller controller, IEnumerable<T> model)
        {
            if (model != null && model.Any())
                return controller.Ok(model);

            return controller.NotFound();
        }
    }
}