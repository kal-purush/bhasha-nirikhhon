using System.Data.Common;
using System.Data.SqlClient;
using System.Text;
using Dapper;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RestaurauntApp.Services.Classes;

namespace RestaurauntApp.Middlewares
{
    public class LoggerMD
    {
        private readonly ILogger<LoggerMD> logger;
        private readonly RequestDelegate next;
        private readonly IConfiguration configuration;

        public LoggerMD(RequestDelegate next, ILogger<LoggerMD> logger, IConfiguration configuration)
        {
            this.next = next;
            this.logger = logger;
            this.configuration = configuration;
        }
        public async Task InvokeAsync(HttpContext context, RequestDelegate next)
        {
            var isEnabled = configuration.GetValue<bool>("Logging:EnableLogging");
            if (isEnabled)
            {
                var logEntry = new LogEntry
                {
                    UserId = context.User.Identity.Name, // с аутентификацией
                    Url = context.Request.Path,
                    Method = context.Request.Method,
                    StatusCode = context.Response.StatusCode,
                    RequestBody = "", //  логика для записи тела request
                    ResponseBody = "", //логика для записи тела response
                    Timestamp = DateTime.UtcNow
                };

                using (var connection = new SqlConnection("Server=localhost;Database=RestaurantAppDb;Integrated Security=SSPI"))
                {
                    await connection.ExecuteAsync("INSERT INTO LogEntries (UserId, Url, Method, StatusCode, RequestBody, ResponseBody, Timestamp) VALUES (@UserId, @Url, @Method, @StatusCode, @RequestBody, @ResponseBody, @Timestamp)", logEntry);
                }
            }
            await next.Invoke(context);
        }
    }
}