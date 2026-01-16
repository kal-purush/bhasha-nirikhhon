using System;
using System.Threading.Tasks;
using BookLib.Helpers;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.Net.Http.Headers;

namespace BookLib.Filter
{
    //检查请求消息头中是否包含If-Match项，如果没有返回400 Bad Request
    public class CheckIfMatchHeaderFilterAttribute: ActionFilterAttribute
    {
        public override Task OnActionExecutionAsync(ActionExecutingContext context, ActionExecutionDelegate next)
        {
            if (!context.HttpContext.Request.Headers.ContainsKey(HeaderNames.IfMatch))
            {
                context.Result = new BadRequestObjectResult(new ApiError
                {
                    Message = "The request must have If-Match header"
                });
            }
            return base.OnActionExecutionAsync(context, next);

        }
    }
}