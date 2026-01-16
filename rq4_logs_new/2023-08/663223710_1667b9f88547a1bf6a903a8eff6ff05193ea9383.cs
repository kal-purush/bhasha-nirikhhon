using FluentValidation;
using Hellang.Middleware.ProblemDetails;
using ProblemDetailsOptions = Hellang.Middleware.ProblemDetails.ProblemDetailsOptions;

namespace Fossa.API.Web;

public static class ProblemDetailsOptionsExtensions
{
  public static void MapFluentValidationException(
    this ProblemDetailsOptions options) =>
      options.Map<ValidationException>((ctx, ex) =>
      {
        var factory = ctx.RequestServices.GetRequiredService<ProblemDetailsFactory>();

        var errors = ex.Errors
                  .GroupBy(x => x.PropertyName, StringComparer.OrdinalIgnoreCase)
                  .ToDictionary(
                      x => x.Key,
                      x => x.Select(x => x.ErrorMessage).ToArray(), StringComparer.OrdinalIgnoreCase);

        return factory.CreateValidationProblemDetails(ctx, errors);
      });
}