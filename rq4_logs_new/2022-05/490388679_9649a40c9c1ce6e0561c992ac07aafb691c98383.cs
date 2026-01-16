using System.Net;
using Blog.Data;
using Blog.Dtos.PostDtos;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace Blog.Controllers;

[ApiController]
[Route("/api")]

public class PostController : HomeController
{
    [HttpGet]
    [Route("posts")]
    public async Task<IActionResult> GetAllAsync(
        [FromServices] BlogDataContext context,
        [FromQuery] int? page = 0,
        [FromQuery] int? pageSize = 25)
    {
        if(page is null || pageSize is null)
            return FailureResponse(
                (int)HttpStatusCode.BadRequest,
                "The page and page size fields is required.");

        var countPosts = await context.Posts.AsNoTracking().CountAsync();

        var posts = await context
            .Posts
            .AsNoTracking()
            .Include(post => post.Category)
            .Include(post => post.Author)
            .Select(x => new ListPostsDto
            {
                Id = x.Id,
                Title = x.Title,
                LastUpdateDate = x.LastUpdateDate.ToString(),
                Category = x.Category.Name,
                Author = $"{x.Author.Name} ({x.Author.Email})"
            })
            .Skip((int)page * (int)pageSize)
            .Take((int)pageSize)
            .OrderByDescending(post => post.LastUpdateDate)
            .ToListAsync();

        var response = new ListPostsResponseDto(
            total: countPosts,
            page: (int)page,
            pageSize: (int)pageSize,
            posts: posts
        );

        return SuccessResponse<ListPostsResponseDto>(
            statusCode: (int)HttpStatusCode.OK,
            value: response);
    }
}