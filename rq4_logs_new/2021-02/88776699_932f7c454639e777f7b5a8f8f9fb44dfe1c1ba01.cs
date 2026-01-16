using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Serilog.Context;

namespace AnlabMvc.Middleware
{
  public class LogUserNameMiddleware
  {
    private readonly RequestDelegate _next;

    public LogUserNameMiddleware(RequestDelegate next)
    {
      _next = next;
    }

    public async Task Invoke(HttpContext context)
    {
      using (LogContext.PushProperty("User", context.User.Identity.Name ?? "anonymous"))
      {
        await _next(context);
      }
    }
  }
}