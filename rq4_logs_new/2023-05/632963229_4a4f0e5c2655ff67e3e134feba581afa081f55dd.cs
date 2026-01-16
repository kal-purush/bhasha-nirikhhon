using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using MovieFiles.Core.Interfaces;
using System;
using System.Net;
using System.Threading.Tasks;

namespace MovieFiles.Api.Functions
{
    public class Users
    {
        private readonly ILogger<Users> _logger;
        private readonly IUserRepository _userRepository;

        public Users(ILogger<Users> log, IUserRepository userRepository)
        {
            _logger = log;
            _userRepository = userRepository;
        }

        [FunctionName("ResolveUser")]
        [OpenApiOperation(operationId: "ResolveUser", tags: new[] { "Users" })]
        [OpenApiParameter(name: "userId", In = ParameterLocation.Path, Required = true, Type = typeof(Guid))]
        [OpenApiParameter(name: "username", In = ParameterLocation.Path, Required = true, Type = typeof(string))]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(void))]
        [OpenApiParameter(name: "x-functions-key", In = ParameterLocation.Header, Required = true, Type = typeof(string), Description = "The function key")]

        public async Task<IActionResult> ResolveUser(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = "users/resolve/{userId}/{username}")] HttpRequest req,
            string userId,
            string username)
        {
            _logger.LogInformation($"ResolveUser function processed a request for user {userId}.");

            // Convert string parameter to Guid
            if (!Guid.TryParse(userId, out var userIdGuid))
            {
                return new BadRequestObjectResult("Invalid user ID.");
            }

            await _userRepository.ResolveUser(userIdGuid, username);

            return new OkResult();
        }
    }
}