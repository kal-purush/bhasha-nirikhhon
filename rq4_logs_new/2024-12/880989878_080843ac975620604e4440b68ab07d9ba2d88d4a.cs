using Microsoft.AspNetCore.Mvc;
using System.Net;

namespace MarGate.Core.Api.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class BaseController(ILogger<BaseController> logger) : ControllerBase //abstract yapmalı mıyız
    {
        private readonly ILogger<BaseController> _logger = logger;

        protected IActionResult ApiResponse(object data, HttpStatusCode statusCode = HttpStatusCode.OK, string message = null)
        {
            var response = new ApiResponse
            {
                StatusCode = (int)statusCode,
                Data = data,
                Message = message,
                Success = statusCode == HttpStatusCode.OK
            };

            _logger.LogInformation("Successfully returned data: {Data}", data);

            return StatusCode((int)statusCode, response);
        }

        protected IActionResult HandleError(Exception ex)
        {
            _logger.LogError(ex, "An error occurred while processing the request.");

            var response = new ApiResponse
            {
                StatusCode = (int)HttpStatusCode.InternalServerError,
                Data = null,
                Message = ex.Message,
                Success = false
            };

            return StatusCode((int)HttpStatusCode.InternalServerError, response);
        }

        protected async Task<IActionResult> HandleErrorAsync(Exception ex)
        {
            _logger.LogError(ex, "An error occurred while processing the request asynchronously.");

            var response = new ApiResponse
            {
                StatusCode = (int)HttpStatusCode.InternalServerError,
                Data = null,
                Message = ex.Message,
                Success = false
            };

            return await Task.FromResult(StatusCode((int)HttpStatusCode.InternalServerError, response));
        }

        protected IActionResult NoContentResponse()
        {
            _logger.LogInformation("No content available, returning HTTP 204.");

            return StatusCode((int)HttpStatusCode.NoContent, new ApiResponse
            {
                StatusCode = (int)HttpStatusCode.NoContent,
                Data = null,
                Message = "No content available.",
                Success = true
            });
        }
    }

    // Standart API yanıt sınıfı
    public class ApiResponse
    {
        public int StatusCode { get; set; }
        public object? Data { get; set; } // data object mi olmalı data ve message nullable mı olmalı
        public string? Message { get; set; }
        public bool Success { get; set; }
    }
}
