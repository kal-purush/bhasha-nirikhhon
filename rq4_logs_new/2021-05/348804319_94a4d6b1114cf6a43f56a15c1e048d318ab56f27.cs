using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TestDiplom.Models;
using TestDiplom.Models.Comment;

namespace TestDiplom.Controllers.Comment
{
    [Route("api/[controller]")]
    [ApiController]
    public class CommentController : ControllerBase
    {
        private readonly AuthenticationContext _context;
        public CommentController(AuthenticationContext context)
        {
            _context = context;
        }

        [HttpPost]
        [Authorize]
        [Route("Create")]
        //POST : /api/Comment/Create
        public async Task<Object> Create(CommentModel model)
        {
            string userId = User.Claims.First(c => c.Type == "UserID").Value;

            var comment = new Models.Comment.Comment()
            {
                StudentPracticeId = model.StudentPracticeId,
                OwnerId = userId,
                CommentContent = model.CommentContent,
                CreationTime = DateTime.Now
            };
            try
            {
                await _context.Comments.AddAsync(comment);
                await _context.SaveChangesAsync();

                return Ok();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        [HttpGet]
        [Authorize]
        [Route("GetAllById")]
        //GET : /api/Comment/GetAllById
        public async Task<List<CommentModel>> GetAllById(string Id)
        {
            var id = Convert.ToInt32(Id);

            var list = await _context.Comments
                .Include(c => c.OwnerFk)
                .Where(c => c.StudentPracticeId == id).ToListAsync();

            var lst = new List<CommentModel>();

            foreach (var item in list)
            {
                var comment = new CommentModel();

                comment.Id = item.Id;
                comment.StudentPracticeId = item.StudentPracticeId;
                comment.OwnerId = item.OwnerId;
                comment.OwnerName = item.OwnerFk.FullName;
                comment.CommentContent = item.CommentContent;
                comment.CreationTime = item.CreationTime;

                lst.Add(comment);
            }

            return lst;
        }
    }
}