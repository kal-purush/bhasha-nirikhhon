using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace WebAPI.Middlewares;

public class GlobalExceptionHandlerMiddleware : IMiddleware
{
    private readonly IWebHostEnvironment _env;

    public GlobalExceptionHandlerMiddleware(IWebHostEnvironment env)
        => _env = env;
    
    public async Task InvokeAsync(HttpContext context, RequestDelegate next)
    {
        var title = string.Empty;
        var detail = string.Empty;
        
        try
        {
            await next(context);
        }
        catch (DbUpdateException e)
        {
            if (_env.IsDevelopment())
            {
                title = e.Message;
                detail = e.InnerException!.Message + Environment.NewLine + e.StackTrace;
            }

            if (_env.IsProduction())
            {
                title = "Database Server Error";
                detail = "Something wrong happens on database:/";
            }
            
            await SetErrorResponse(
                new ProblemDetails
                {
                    Status = StatusCodes.Status500InternalServerError,
                    Type = "https://datatracker.ietf.org/doc/html/rfc7235#section-3.1",
                    Title = title,
                    Detail = detail
                },
                context
            );
        }
        catch (UnauthorizedAccessException e)
        {
            await SetErrorResponse(
                new ProblemDetails
                {
                    Status = StatusCodes.Status401Unauthorized,
                    Type = "https://datatracker.ietf.org/doc/html/rfc7235#section-3.1",
                    Title = e.Message,
                    Detail = "The logged-in user and the user whose data needs to be updated are different individuals."
                },
                context
            );
        }
        catch (Exception e)
        {
            if (_env.IsDevelopment())
            {
                title = e.Message;
                detail = e.InnerException!.Message + Environment.NewLine + e.StackTrace;
            }

            if (_env.IsProduction())
            {
                title = "Internal Server Error";
                detail = "Something definitely went wrong on application server :/";
            }

            await SetErrorResponse(
                new ProblemDetails
                {
                    Status = StatusCodes.Status500InternalServerError,
                    Type = "https://datatracker.ietf.org/doc/html/rfc7231#section-6.6.1",
                    Title = title,
                    Detail = detail
                },
                context
            );
        }
    }

    private static async Task SetErrorResponse(ProblemDetails problemDetails, HttpContext context)
    {
        context.Response.StatusCode = (int)problemDetails.Status!;
        
        string json = JsonSerializer.Serialize(problemDetails);

        context.Response.ContentType = "application/json";

        await context.Response.WriteAsync(json);
    }
}