using System.Net.Http.Json;
using Shared;

namespace FrontendBlazorWebAssembly.Services;

public class CommentService : ICommentService
{
    
    private readonly HttpClient _httpClient;
     
    public CommentService(HttpClient httpClient)
    {
        _httpClient = httpClient;
    }
    
    
    
    
    public async Task AddCommentToMovie(int userid, long movieid, string comment)
    {
        var response = await _httpClient.PostAsJsonAsync(
            $"/Comment?userid={userid}&movieid={movieid}&comment={comment}",
            new { } // Add your serialized object here
        );
        
        response.EnsureSuccessStatusCode();
    }

    
    
    
    public async Task<List<UserComment>> GetCommentsByMovieId(long movieid)
    {
        var commentsByMovieId = await _httpClient.GetFromJsonAsync<List<UserComment>>($"/Comment?moviedid={movieid}");
        return commentsByMovieId;
    }
}