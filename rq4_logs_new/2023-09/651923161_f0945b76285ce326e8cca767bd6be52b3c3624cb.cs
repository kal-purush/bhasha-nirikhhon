using System.Security.Claims;
using API.Entities;
using API.Interfaces;
using Microsoft.AspNetCore.Mvc;

namespace API.Controllers
{
    public class PostsController : BaseApiController
    {
        private readonly IUnitOfWork _unitOfWork;
        public PostsController(IUnitOfWork unitOfWork)
        {
            _unitOfWork = unitOfWork;
        }

        [HttpGet]
        public async Task<ActionResult<IEnumerable<Post>>> GetPosts()
        {
            var posts = await _unitOfWork.PostRepository.GetAllPosts();
            if(posts == null) return NoContent();
            return Ok(posts);
        }

        [HttpPost]
        public async Task<ActionResult<Post>> CreatePost([FromBody] Post post)
        {
            if(post == null)
            {
                return BadRequest();
            }

            var userId = User.FindFirst(ClaimTypes.NameIdentifier)?.Value;
            post.UserId = Int32.Parse(userId);

            await _unitOfWork.PostRepository.CreatePost(post);

            return CreatedAtAction(nameof(GetPosts), new {id = post.Id }, post);

        }
    }
}