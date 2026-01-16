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
using System;
using System.Collections.Generic;
using MovieFiles.Core.Models;


namespace MovieFiles.Api.Functions
{
    public class MovieLists
    {
        private readonly ILogger<MovieLists> _logger;
        private readonly IMovieListService _movieListService;

        public MovieLists(ILogger<MovieLists> log, IMovieListService movieListService)
        {
            _logger = log;
            _movieListService = movieListService;
        }

        [FunctionName("AddMovieToWatchLater")]
        [OpenApiOperation(operationId: "AddMovieToWatchLater", tags: new[] { "Movie lists" })]
        [OpenApiParameter(name: "userId", In = ParameterLocation.Query, Required = true, Type = typeof(Guid), Description = "Id of user that is adding movie to his list")]
        [OpenApiParameter(name: "movieId", In = ParameterLocation.Query, Required = true, Type = typeof(int), Description = "Id of movie to add to the list")]
        [OpenApiParameter(name: "x-functions-key", In = ParameterLocation.Header, Required = true, Type = typeof(string), Description = "The function key")]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "text/plain", bodyType: typeof(string), Description = "The OK response")]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.BadRequest, contentType: "text/plain", bodyType: typeof(string), Description = "Incorrect parameters were provided.")]
        public async Task<IActionResult> AddMovieToWatchLater(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req)
        {
            _logger.LogInformation($"Get Movies to watch later triggered with userId {req.Query["userId"]} and movie id {req.Query["movieId"]}");

            if (!int.TryParse(req.Query["movieId"], out var movieId) && movieId <1){
                return new BadRequestObjectResult("Invalid movie ID.");
            }
            if (!Guid.TryParse(req.Query["userId"], out var userId)){
                return new BadRequestObjectResult("Invalid user ID.");
            }

            var additionSuccesful = await _movieListService.AddMovieToWatchLater(movieId, userId);

            if (additionSuccesful){
                return new OkObjectResult("Saved");
            } else {
                return new BadRequestObjectResult("this movie already exists or it is unable to add it to the list.");
            }

            
        }

        [FunctionName("GetMoviesToWatchLater")]
        [OpenApiOperation(operationId: "GetMoviesToWatchLater", tags: new[] { "Movie lists" })]
        [OpenApiParameter(name: "userId", In = ParameterLocation.Query, Required = true, Type = typeof(Guid), Description = "Id of a user to get the list of.")]
        [OpenApiParameter(name: "page", In = ParameterLocation.Query, Required = true, Type = typeof(int), Description = "Page number to return.")]
        [OpenApiParameter(name: "x-functions-key", In = ParameterLocation.Header, Required = true, Type = typeof(string), Description = "The function key")]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(CustomMovieList<WatchLaterMovie>), Description = "The OK response")]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.BadRequest, contentType: "text/plain", bodyType: typeof(string), Description = "Incorrect parameters were provided.")]
        public async Task<IActionResult> GetMoviesToWatchLater(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req)
        {
            _logger.LogInformation($"Get Movies to watch later triggered with userId {req.Query["userId"]}");

            if (!Guid.TryParse(req.Query["userId"], out var userId)){
                return new BadRequestObjectResult("Invalid user ID.");
            }

            if (!int.TryParse(req.Query["page"], out var page) && page <1){
                return new BadRequestObjectResult("Invalid page number.");
            }

            CustomMovieList<WatchLaterMovie> list = await _movieListService.GetWatchLaterList(userId,page);

            return new OkObjectResult(list);
        }

        [FunctionName("RemoveMovieToWatchLater")]
        [OpenApiOperation(operationId: "RemoveMovieToWatchLater", tags: new[] { "Movie lists" })]
        [OpenApiParameter(name: "userId", In = ParameterLocation.Query, Required = true, Type = typeof(Guid), Description = "Id of user that is adding movie to his list")]
        [OpenApiParameter(name: "movieId", In = ParameterLocation.Query, Required = true, Type = typeof(int), Description = "Id of movie to add to the list")]
        [OpenApiParameter(name: "x-functions-key", In = ParameterLocation.Header, Required = true, Type = typeof(string), Description = "The function key")]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "text/plain", bodyType: typeof(string), Description = "The OK response")]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.BadRequest, contentType: "text/plain", bodyType: typeof(string), Description = "Incorrect parameters were provided.")]
        public async Task<IActionResult> RemoveMovieToWatchLater(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req)
        {
            _logger.LogInformation($"Get Movies to watch later triggered with userId {req.Query["userId"]} and movie id {req.Query["movieId"]}");

            if (!int.TryParse(req.Query["movieId"], out var movieId) && movieId <1){
                return new BadRequestObjectResult("Invalid movie ID.");
            }
            if (!Guid.TryParse(req.Query["userId"], out var userId)){
                return new BadRequestObjectResult("Invalid user ID.");
            }

            var deletionSuccesfull = await _movieListService.RemoveMovieToWatchLater(movieId, userId);

            if (deletionSuccesfull){
                return new OkObjectResult("Saved");
            } else {
                return new NotFoundObjectResult("Unable to find resource to delete");
            }
            
        }
    }
}
