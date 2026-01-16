using BookStore.BusinessLogic.IServices;
using BookStore.Common;
using BookStore.Domain;
using System.Linq;
using System.Threading.Tasks;
using System.Web.Mvc;

namespace BookStore.Presentation.Areas.Admin.Controllers
{
    public class FeedbackAdminController : Controller
    {
        private readonly IFeedbackServices _feedback;

        public FeedbackAdminController(IFeedbackServices feedback)
        {
            _feedback = feedback;
        }

        [Route("trang-quan-tri/quan-ly-phan-hoi")]
        public async Task<ActionResult> FeedbackList()
        {
            if (Base.Account == null || Base.Account.Role != RoleUser.Admin)
            {
                return Redirect("/");
            }
            var feedbacks = await _feedback.GetAllAsync();
            return View("~/Areas/Admin/Views/FeedbackAdmin/FeedbackList.cshtml", feedbacks.OrderByDescending(x => x.CreatedDate));
        }
    }
}