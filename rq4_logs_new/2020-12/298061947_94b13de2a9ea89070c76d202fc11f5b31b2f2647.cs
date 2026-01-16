using System;
using System.Collections.Generic;
using System.Text;
using System.Web.Mvc;
using SmartSaver.Domain.CustomExceptions;

namespace SmartSaver.Domain.ActionFilters
{
    public class CheckForInvalidModelAttribute : ActionFilterAttribute
    {
        public CheckForInvalidModelAttribute() {}

        public override void OnActionExecuting(ActionExecutingContext filterContext)
        {
            if (!filterContext.Controller.ViewData.ModelState.IsValid)
            {
                throw new InvalidModelException();
            }
        }
    }
}