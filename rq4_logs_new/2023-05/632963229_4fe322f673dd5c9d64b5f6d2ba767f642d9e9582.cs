using System.IO;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Enums;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Newtonsoft.Json;
using MovieFiles.Core.Interfaces;
using MovieFiles.Core.Models;
using System.Collections.Generic;

namespace MovieFiles.Api.Functions
{
    public class MovieGenre
    {
        private readonly ILogger<MovieGenre> _logger;
        private readonly IMovieUtilService _utilService;

        public MovieGenre(ILogger<MovieGenre> log, IMovieUtilService utilService)
        {
            _logger = log;
            _utilService = utilService;
        }

        [FunctionName("MovieGenre")]
        [OpenApiOperation(operationId: "GetAllGenre", tags: new[] { "Genre" })]
        [OpenApiSecurity("function_key", SecuritySchemeType.ApiKey, Name = "code", In = OpenApiSecurityLocationType.Query)]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "text/plain", bodyType: typeof(GenreList), Description = "The OK response")]
        public async Task<IActionResult> GetAllGenre(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req)
        {
            _logger.LogInformation("C# HTTP trigger function processed a request to get all genre.");

            return new OkObjectResult(await _utilService.GetGenresAsync());
        }
    }
}
