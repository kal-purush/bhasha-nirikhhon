using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using MovieFiles.Core.Interfaces;
using MovieFiles.Core.Models;


namespace MovieFiles.Api.Functions.Movies;

public class MovieUtilFunction
{
    private readonly IMovieUtilService _movieUtilService;

    public MovieUtilFunction(IMovieUtilService movieUtilService)
    {
        _movieUtilService = movieUtilService;
    }

    // For GetGenres:
    [FunctionName("GetGenres")]
    [OpenApiOperation(operationId: "GetGenres", tags: new[] { "MovieUtil" })]
    [OpenApiParameter(name: "x-functions-key", In = ParameterLocation.Header, Required = true, Type = typeof(string),
        Description = "The function key")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(GenreList),
        Description = "The OK response")]
    public async Task<IActionResult> GetGenres(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = "genre/movie/list")]
        HttpRequest req)
    {
        return new OkObjectResult(await _movieUtilService.GetGenresAsync());
    }
    
}