using AutoMapper;
using DAL;
using DAL.Entities;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using ProgrammingForum_ASPNETCore.Models.PostModels;
using ProgrammingForum_ASPNETCore.Models.PostReplyModels;

namespace ProgrammingForum_ASPNETCore.Controllers
{
    public class PostReplyController : Controller
    {
        private readonly AppDbContext _context;
        private readonly IMapper _mapper;
        public PostReplyController(AppDbContext context, IMapper mapper)
        {
            _context = context;
            _mapper = mapper;
        }

        //[Authorize]
        [HttpPost]
        public IActionResult CreatePostReply(PostReplyCreateModel replyCreateModel)
        {
            replyCreateModel.AuthorName = User.Identity.Name;

            var postReply = _mapper.Map<PostReply>(replyCreateModel);
            //var postReplyView = _mapper.Map<PostReplyViewModel>(postReply);


            _context.PostReplies.Add(postReply);
            _context.SaveChanges();


            List<PostReply> postReplies = _context.PostReplies.Where(pr => pr.PostId == postReply.PostId).ToList();
            List<PostReplyViewModel> repliesView = new List<PostReplyViewModel>();
            foreach (var reply in postReplies)
            {
                var replyView = _mapper.Map<PostReplyViewModel>(reply);
                repliesView.Add(replyView);
            }
            
            return PartialView("_PostRepliesPartial", repliesView);
        }
    }
}