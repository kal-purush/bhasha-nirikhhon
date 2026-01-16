using ActivityFinder.Server.Database;
using ActivityFinder.Server.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace ActivityFinder.Server.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    [Authorize]
    public class CommentController : ControllerBase
    {
        private readonly ILogger<CommentController> _logger;
        private readonly ICommentService _commentService;
        private readonly IUserService _userService;

        public CommentController(ILogger<CommentController> logger, AppDbContext context)
        {
            _logger = logger;
            _commentService = new CommentService(context);
            _userService = new UserService(context);
        }

        [HttpPost]
        public IActionResult Create(string content)
        {
            if (string.IsNullOrEmpty(content))
                return BadRequest("Brak treści komentarza");

            var userResult = _userService.GetByName(User.Identity.Name);

            if (!userResult.Success)
                return NotFound(userResult.Message);

            var createResult = _commentService.AddComment(CommentMapper.ToComment(content, userResult.Value));

            if (!createResult.Success)
                return BadRequest(createResult.Message);

            return Ok(CommentMapper.ToVm(createResult.Value));
        }
    }
}