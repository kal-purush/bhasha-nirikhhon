using System;
using System.Net.Mime;
using System.Threading.Tasks;
using FluentValidation;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.AspNetCore.Mvc.Formatters;
using Microsoft.Extensions.DependencyInjection;

namespace F2.Src.Presentation.Filters.Validation;

public sealed class F2ValidationFilter : IAsyncActionFilter
{
    private readonly IValidator<F2Request> _validator;

    public F2ValidationFilter(IValidator<F2Request> validator)
    {
        _validator = validator;
    }

    public async Task OnActionExecutionAsync(
        ActionExecutingContext context,
        ActionExecutionDelegate next
    )
    {
        const string RequestKey = "request";

        var request = context.ActionArguments[RequestKey] as F2Request;

        var result = await _validator.ValidateAsync(request);

        if (!result.IsValid)
        {
            context.Result = new ContentResult
            {
                StatusCode = StatusCodes.Status400BadRequest,
                Content = "Invalid request, validation failed",
                ContentType = MediaTypeNames.Application.Json,
            };

            return;
        }

        await next();
    }
}