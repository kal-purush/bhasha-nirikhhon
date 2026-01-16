using System.Security.Claims;
using team_scriptslingers_backend.Models;
using team_scriptslingers_backend.Repositories;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace team_scriptslingers_backend.Controllers
{

    [ApiController]
    [Route("api/[controller]")]
    public class CommentController : ControllerBase
    {
        private readonly ILogger<CommentController> _logger;
        private readonly ICommentRepository _commentRepository;
        private readonly IAuthRepository _authService;

        public CommentController(ILogger<CommentController> logger, ICommentRepository repository, IAuthRepository service)
        {
            _logger = logger;
            _commentRepository = repository;
            _authService = service;
        }

        [HttpGet]
        public ActionResult<IEnumerable<Comment>> GetAllComments()
        {
            return Ok(_commentRepository.GetAllComments());
        }

        [HttpGet]
        [Route("{commentId:int}")]
        public ActionResult<Comment> GetCommentById(int commentId)
        {
            var comment = _commentRepository.GetCommentById(commentId);
            if (comment == null)
            {
                return NotFound();
            }
            return Ok(comment);
        }

        [HttpPost]
        [Authorize(AuthenticationSchemes = JwtBearerDefaults.AuthenticationScheme)]
        public ActionResult<Comment> CreateComment(Comment newComment)
        {
            if (!ModelState.IsValid || newComment == null)
            {
                return BadRequest();
            }

            var UserName = HttpContext.User.FindFirst(ClaimTypes.Email)?.Value;

            if (UserName == null)
            {
                return Unauthorized();
            }

            var createdComment = new Comment
            {
                commentContent = newComment.commentContent,
                commenter = UserName,
                postedAt = DateTime.Now,
                postId = newComment.postId
            };

            var newCreatedComment = _commentRepository.CreateComment(createdComment);

            return Created(nameof(GetCommentById), newCreatedComment);
        }

        [HttpPut]
        [Route("{commentId:int}")]
        [Authorize(AuthenticationSchemes = JwtBearerDefaults.AuthenticationScheme)]
        public ActionResult<Comment> EditComment(int commentId, Comment comment)
        {
            var existingComment = _commentRepository.GetCommentById(commentId);

            if (existingComment == null)
            {
                return NotFound();
            }

            if (comment == null)
            {
                return BadRequest();
            }

            if (!ModelState.IsValid || comment == null)
            {
                return BadRequest();
            }

            var UserName = HttpContext.User.FindFirst(ClaimTypes.Email)?.Value;

            if (UserName == null)
            {
                return Unauthorized();
            }

            existingComment.commentContent = comment.commentContent;
            existingComment.commenter = UserName;
            existingComment.postedAt = DateTime.Now;

            return Ok(_commentRepository.EditComment(existingComment)
            );
        }

        [HttpDelete]
        [Route("{commentId:int}")]
        [Authorize(AuthenticationSchemes = JwtBearerDefaults.AuthenticationScheme)]
        public ActionResult DeleteComment(int commentId)
        {
            var existingComment = _commentRepository.GetCommentById(commentId);
            if (existingComment == null){
                return NotFound();
            }
            var userName = HttpContext.User.FindFirst(ClaimTypes.Email)?.Value;

            if (existingComment.commenter != userName){
                return Unauthorized();
            }      

            _commentRepository.DeleteComment(commentId);
            return NoContent();
        }
    }
}