using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http.Internal;

namespace LoggingMiddleware.Helpers
{
    public class DetailedLoggingMiddleware
    {
        private readonly ILogger<DetailedLoggingMiddleware> _logger;
        private readonly RequestDelegate _next;

        public DetailedLoggingMiddleware(RequestDelegate next, ILogger<DetailedLoggingMiddleware> logger)
        {
            _next = next;
            _logger = logger;
        }

        public async Task Invoke(HttpContext context)
        {
            using (var responseBody = new MemoryStream())
            {
                //handle response
                var originalBodyStream = context.Response.Body;
                context.Response.Body = responseBody;
                try
                {
                    //log request
                    _logger.LogDebug(await FormatRequest(context.Request));

                    await _next(context);

                    _logger.LogDebug(await FormatResponse(context.Response));
                    await responseBody.CopyToAsync(originalBodyStream);
                }
                finally
                {
                    context.Response.Body = originalBodyStream;
                }
            }
        }

        private async Task<string> FormatResponse(HttpResponse response)
        {
            response.Body.Seek(0, SeekOrigin.Begin);

            string text = await new StreamReader(response.Body).ReadToEndAsync();

            response.Body.Seek(0, SeekOrigin.Begin);

            //Return the string for the response, including the status code (e.g. 200, 404, 401, etc.)
            return $"{response.StatusCode}: {text}";
        }

        private async Task<string> FormatRequest(HttpRequest request)
        {
            request.EnableRewind();

            var buffer = new byte[Convert.ToInt32(request.ContentLength)];

            await request.Body.ReadAsync(buffer, 0, buffer.Length).ConfigureAwait(false);
            var bodyAsText = Encoding.UTF8.GetString(buffer);

            request.Body.Position = 0;

            return $"{request.Scheme} {request.Host}{request.Path} {request.QueryString} {bodyAsText}";
        }
    }
}