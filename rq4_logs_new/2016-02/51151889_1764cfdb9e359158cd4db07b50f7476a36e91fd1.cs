namespace BoardGames.Areas.Private.Controllers
{
    using Microsoft.AspNet.Identity;
    using Models;
    using Services.Data.Contracts;
    using System.Linq;
    using System.Web.Mvc;
    using Web.Infrastructure.Mapping;
    [Authorize]
    public class CommentsController : Controller
    {
        private readonly ICommentsService comments;

        public CommentsController(ICommentsService comments)
        {
            this.comments = comments;
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Add(int tutorialId, int page, CommentCreateViewModel model)
        {
            this.comments.Add(model.Content, User.Identity.GetUserId(), tutorialId);

            var result = this.comments.GetByPage(tutorialId, page).To<CommentViewModel>().ToList();

            return this.PartialView("_CommentsPartial", result);
        }
        
        public ActionResult GetByPage(int tutorialId, int page)
        {
            var result = this.comments.GetByPage(tutorialId, page).To<CommentViewModel>().ToList();

            return this.PartialView("_CommentsPartial", result);
        }
    }
}