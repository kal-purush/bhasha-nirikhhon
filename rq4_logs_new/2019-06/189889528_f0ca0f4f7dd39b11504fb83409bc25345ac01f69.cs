using BethanysPieShop.Web.EFModels;
using BethanysPieShop.Web.Models;
using BethanysPieShop.Web.ViewModels;
using Microsoft.AspNetCore.Mvc;

namespace BethanysPieShop.Web.Controllers
{
    public class FeedbackController : Controller
    {
        private readonly IFeedbackRepository _feedbackRepository;

        public FeedbackController(IFeedbackRepository feedbackRepository)
        {
            _feedbackRepository = feedbackRepository;
        }

        public IActionResult Index()
        {
            return View();
        }

        [HttpPost]
        public IActionResult Index(FeedbackViewModel feedbackViewModel)
        {
            if (ModelState.IsValid)
            {
                Feedback feedback = new Feedback
                {
                    Name = feedbackViewModel.Name,
                    Email = feedbackViewModel.Email,
                    Message = feedbackViewModel.Message,
                    ContactMe = feedbackViewModel.ContactMe
                };

                _feedbackRepository.AddFeedback(feedback);
                return RedirectToAction("FeedbackComplete");
            }
            return View(feedbackViewModel);
        }

        public IActionResult FeedbackComplete()
        {
            return View();
        }
    }
}