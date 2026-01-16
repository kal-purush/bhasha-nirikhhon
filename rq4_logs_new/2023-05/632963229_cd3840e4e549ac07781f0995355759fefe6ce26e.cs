
using System.Collections.Generic;
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
using System.Net.Http;
using System.Text.Json;
using MovieFiles.Core.Models;
using MovieFiles.Core.Interfaces;


namespace MovieFiles.Api.Functions.Movies;

public class Movies1
{

    private readonly IMoviesService _moviesService;
    
    public Movies1(IMoviesService moviesService)
    {
        _moviesService = moviesService;
    }
    
    [FunctionName("GetLatestMovies")]
    [OpenApiOperation(operationId: "GetLatestMovies", tags: new[] { "Movies" })]
    [OpenApiParameter(name: "page", In = ParameterLocation.Path, Required = false, Type = typeof(int))]
    [OpenApiParameter(name: "x-functions-key", In = ParameterLocation.Header, Required = true, Type = typeof(string), Description = "The function key")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(MovieList), Description = "The OK response")]
    public async Task<(IList<Movie>?, HttpResponseMessage?)> GetLatestMovies()
    {
        
    }
     
    public async Task<(IList<Movie>?, HttpResponseMessage?)> GetNowPlayingMoviesAsync()
    {
        
    }
    
    public async Task<(IList<Movie>?, HttpResponseMessage?)> GetPopularMoviesAsync()
    {
        
    }
    
    public async Task<(IList<Movie>?, HttpResponseMessage?)> GetTopRatedMoviesAsync()
    {
        
    }
    
    public async Task<(IList<Movie>?, HttpResponseMessage?)> GetUpcomingMoviesAsync()
    {
        
    }
    
    
}