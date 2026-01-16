using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.Extensions.Configuration;
using P2PLoan.Utils;
using System.IO;
using System.Linq;

namespace P2PLoan.Attributes;

public class ValidateMonnifySignatureAttribute : ActionFilterAttribute
{
    private readonly IConfiguration _configuration;

    public ValidateMonnifySignatureAttribute(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    public override void OnActionExecuting(ActionExecutingContext context)
    {
        var request = context.HttpContext.Request;

        // Get the raw request body
        request.EnableBuffering();
        request.Body.Position = 0;

        using (var reader = new StreamReader(request.Body))
        {
            var body = reader.ReadToEndAsync().Result;
            request.Body.Position = 0;

            // Compute the hash
            var computedHash = ComputeHash.Compute(body, _configuration["Monnify:SecretKey"]);

            // Get the header value
            var signature = request.Headers["Monnify-Signature"].FirstOrDefault();

            // Validate the signature
            if (computedHash != signature)
            {
                context.Result = new UnauthorizedResult();
            }
        }

        base.OnActionExecuting(context);
    }
}