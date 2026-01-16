using TabloidMVC.Models;
using TabloidMVC.Repositories;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.Xml.Linq;
using TabloidMVC.Models.ViewModels;

namespace TabloidMVC.Controllers
{
    public class CommentController : Controller
    {
        private readonly ICommentRepository _commentRepo;
        private readonly IPostRepository _postRepo;

        public CommentController(ICommentRepository commentRepository, IPostRepository postRepo)

        {
            _commentRepo = commentRepository;
            _postRepo = postRepo;
        }

        // GET: CommentController
        public ActionResult Index(int postId)
        {
            Post post = _postRepo.GetPublishedPostById(postId);
            var viewModel = new PostwithCommentsViewModel()
            {
                Comments = _commentRepo.GetCommentsByPost(postId),
                PostId = postId,
                PostTitle = post.Title
            };
            return View(viewModel);
        }

        // GET: CommentController/Details/5
        //public ActionResult Details(int id)
        //{
        //    Comments comment = _commentRepo.GetCommentById(id);
        //    return View(comment);
        //}

        //// GET: CommentController/Create
        //public ActionResult Create()
        //{
        //    return View();
        //}

        //// POST: CommentController/Create
        //[HttpPost]
        //[ValidateAntiForgeryToken]
        //public ActionResult Create(IFormCollection collection)
        //{
        //    try
        //    {
        //        return RedirectToAction(nameof(Index));
        //    }
        //    catch
        //    {
        //        return View();
        //    }
        //}

        //// GET: CommentController/Edit/5
        //public ActionResult Edit(int id)
        //{
        //    return View();
        //}

        //// POST: CommentController/Edit/5
        //[HttpPost]
        //[ValidateAntiForgeryToken]
        //public ActionResult Edit(int id, IFormCollection collection)
        //{
        //    try
        //    {
        //        return RedirectToAction(nameof(Index));
        //    }
        //    catch
        //    {
        //        return View();
        //    }
        //}

        //// GET: CommentController/Delete/5
        //public ActionResult Delete(int id)
        //{
        //    return View();
        //}

        //// POST: CommentController/Delete/5
        //[HttpPost]
        //[ValidateAntiForgeryToken]
        //public ActionResult Delete(int id, IFormCollection collection)
        //{
        //    try
        //    {
        //        return RedirectToAction(nameof(Index));
        //    }
        //    catch
        //    {
        //        return View(comments);
        //    }
        //}
    }
}