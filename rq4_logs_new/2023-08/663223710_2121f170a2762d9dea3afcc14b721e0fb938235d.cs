using ProblemDetailsOptions = Hellang.Middleware.ProblemDetails.ProblemDetailsOptions;

namespace Fossa.API.Web;

internal static class ProblemDetailsHelper
{
  internal static void ConfigureProblemDetails(ProblemDetailsOptions options)
  {
    options.MapFluentValidationException();

    options.Rethrow<NotSupportedException>();

    options.MapToStatusCode<NotImplementedException>(StatusCodes.Status501NotImplemented);

    options.MapToStatusCode<HttpRequestException>(StatusCodes.Status503ServiceUnavailable);

    options.MapToStatusCode<Exception>(StatusCodes.Status500InternalServerError);
  }
}