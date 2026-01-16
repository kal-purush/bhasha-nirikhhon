using church_mgt_core.services.interfaces;
using church_mgt_models;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace church_mgt_api.Controllers
{
    [ApiController]
    [Route("api/[Controller]")]
    public class CommentsController : ControllerBase
    {
        private readonly ICommentService _commentService;
        private readonly UserManager<AppUser> _userManager;

        public CommentsController(ICommentService commentService, UserManager<AppUser> userManager)
        {
            _commentService = commentService;
            _userManager = userManager;
        }

        [HttpPost]
        public async Task<IActionResult> Index(string comment)
        {
            var user = await _userManager.GetUserAsync(User);
            var result = await _commentService.AddCommentAsync("5dd4e50e-2724-4ccc-9f86-c8b9965b2ddb", comment);
            return StatusCode(result.StatusCode, result);
        }

        [HttpGet()]
        public IActionResult GetComments()
        {
            var result = _commentService.GetAllComments();
            return StatusCode(result.StatusCode, result);
        }

        [HttpGet("{commentId}")]
        public async Task<IActionResult> GetComment(string commentId)
        {
            var result = await _commentService.GetCommentById(commentId);
            return StatusCode(result.StatusCode, result);
        }

        [HttpDelete("{commentId}")]
        public async Task<IActionResult> DeleteComment(string commentId)
        {
            var result = await _commentService.DeleteCommentById(commentId);
            return StatusCode(result.StatusCode, result);
        }
    }
}