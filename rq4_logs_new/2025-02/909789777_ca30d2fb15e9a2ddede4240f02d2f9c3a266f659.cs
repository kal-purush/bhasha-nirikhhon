using FluentValidation.Results;
using Microsoft.AspNetCore.Mvc;
using PetFamily.API.Response;
using PetFamily.Domain.Common;

namespace PetFamily.API.Extensions;

public static class ResponseExtensions
{
    public static ActionResult ToErrorResponse(this Error error)
    {
        var statusCode = error.Type switch
        {
            ErrorType.Validation => StatusCodes.Status400BadRequest,
            ErrorType.NotFound => StatusCodes.Status404NotFound,
            ErrorType.Conflict => StatusCodes.Status409Conflict,
            ErrorType.Failure => StatusCodes.Status500InternalServerError,
            _ => StatusCodes.Status500InternalServerError
        };

        var envelope = Envelop.Error([new ResponseError(error.Code, error.Message, "")]);

        return new ObjectResult(envelope)
        {
            StatusCode = statusCode
        };
    }

    public static ActionResult ToResponse(this ValidationResult validationResult)
    {
        if (validationResult.IsValid)
        {
            throw new InvalidOperationException("Result cannot be successfully validated.");
        }

        var validationErrors = validationResult.Errors;

        var errors = validationErrors.Select(v =>
        {
            var error = Error.Serialize(v.ErrorMessage);

            return new ResponseError(error.Code, error.Message, v.PropertyName);
        });

        var envelope = Envelop.Error(errors);

        return new ObjectResult(envelope) { StatusCode = StatusCodes.Status400BadRequest };
    }
}