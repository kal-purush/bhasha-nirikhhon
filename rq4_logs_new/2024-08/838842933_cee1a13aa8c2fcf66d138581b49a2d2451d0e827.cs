using FakeNews.API.Mocks;
using FakeNews.Application.DTOs.PostDTOs;
using Microsoft.AspNetCore.Mvc;

namespace FakeNews.API.Controllers;

[ApiController]
[Route("api/[controller]")]
public class PostController(MockNewsStorage mockNewsStorage) : ControllerBase
{
    [HttpGet]
    public async Task<ActionResult<PostDto>> GetPost(Guid id)
    {
        try
        {
            return await mockNewsStorage.GetPostById(id);
        }
        catch (Exception e)
        {
            return BadRequest(e.Message);
        }
    }
}