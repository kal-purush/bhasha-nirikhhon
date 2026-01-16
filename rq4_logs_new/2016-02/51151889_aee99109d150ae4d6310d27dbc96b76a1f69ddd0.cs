//namespace BoardGames.Areas.Admin.Controllers
//{
//    using System.Linq;
//    using System.Net;
//    using System.Web.Mvc;
//    using BoardGames.Data.Models;
//    using Microsoft.AspNet.Identity;
//    using Services.Data.Contracts;
//    using Models;
//    using Web.Infrastructure.Mapping;
//    using System.Web;
//    using Common;

//    [Authorize(Roles = GlobalConstants.AdministratorRoleName)]

//    public class ReviewsController : Controller
//    {
//        private readonly IReviewsService reviews;
//        private readonly ICategoriesService categories;
//        private readonly IFilesService files;

//        public ReviewsController(IReviewsService reviews, ICategoriesService categories, IFilesService files)
//        {
//            this.reviews = reviews;
//            this.categories = categories;
//            this.files = files;
//        }

//        // GET: Admin/Reviews
//        public ActionResult Index()
//        {
//            var allReviews = this.reviews.GetAll().To<ReviewViewModel>().ToList();
//            return View(allReviews);
//        }

//        // GET: Admin/Reviews/Details/5
//        public ActionResult Details(int? id)
//        {
//            if (id == null)
//            {
//                return new HttpStatusCodeResult(HttpStatusCode.BadRequest);
//            }

//            Review review = this.reviews.GetById((int)id);
//            if (review == null)
//            {
//                return HttpNotFound();
//            }

//            var viewModel = AutoMapperConfig.Configuration.CreateMapper().Map<ReviewViewModel>(review);
//            return View(viewModel);
//        }

//        // GET: Admin/Reviews/Create
//        public ActionResult Create()
//        {
//            var categories = this.categories.GetAll().To<Models.CategoryViewModel>().ToList();
//            ViewBag.CategoryId = new SelectList(categories, "Id", "Name");
//            return View();
//        }

//        // POST: Admin/Reviews/Create
//        [HttpPost]
//        [ValidateAntiForgeryToken]
//        public ActionResult Create(ReviewCreateViewModel model, HttpPostedFileBase upload)
//        {
//            if (ModelState.IsValid)
//            {
//                int? imageId = null;

//                if (upload != null && upload.ContentLength > 0)
//                {
//                    var imageType = upload.ContentType;
//                    byte[] content = null;
//                    using (var reader = new System.IO.BinaryReader(upload.InputStream))
//                    {
//                        content = reader.ReadBytes(upload.ContentLength);
//                    }

//                    imageId = this.files.Add(imageType, content);
//                }

//                this.reviews.Add(
//                    model.GameTitle,
//                    model.CategoryId,
//                    model.Content,
//                    model.MinPlayers,
//                    model.MaxPlayers,
//                    model.MinAgeRequired,
//                    model.MinPlayingTimeInMinutes,
//                    model.UrlToOfficialSite,
//                    User.Identity.GetUserId(),
//                    imageId);

//                return RedirectToAction("Index");
//            }

//            var categories = this.categories.GetAll().To<Models.CategoryViewModel>().ToList();
//            ViewBag.CategoryId = new SelectList(categories, "Id", "Name");
//            return View(model);
//        }

//        // GET: Admin/Reviews/Edit/5
//        public ActionResult Edit(int? id)
//        {
//            if (id == null)
//            {
//                return new HttpStatusCodeResult(HttpStatusCode.BadRequest);
//            }

//            Review review = this.reviews.GetById((int)id);
//            if (review == null)
//            {
//                return HttpNotFound();
//            }

//            var categories = this.categories.GetAll().To<Models.CategoryViewModel>().ToList();
//            ViewBag.CategoryId = new SelectList(categories, "Id", "Name");

//            var viewModel = AutoMapperConfig.Configuration.CreateMapper().Map<ReviewCreateViewModel>(review);
//            return View(viewModel);
//        }

//        // POST: Admin/Reviews/Edit/5
//        [HttpPost]
//        [ValidateAntiForgeryToken]
//        public ActionResult Edit(ReviewCreateViewModel model, HttpPostedFileBase upload)
//        {
//            if (ModelState.IsValid)
//            {
//                int? imageId = null;

//                if (upload != null && upload.ContentLength > 0)
//                {
//                    var imageType = upload.ContentType;
//                    byte[] content = null;
//                    using (var reader = new System.IO.BinaryReader(upload.InputStream))
//                    {
//                        content = reader.ReadBytes(upload.ContentLength);
//                    }

//                    imageId = this.files.Add(imageType, content);
//                }

//                this.reviews.Edit(
//                    model.Id,
//                    model.GameTitle,
//                    model.CategoryId,
//                    model.Content,
//                    model.MinPlayers,
//                    model.MaxPlayers,
//                    model.MinAgeRequired,
//                    model.MinPlayingTimeInMinutes,
//                    model.UrlToOfficialSite,
//                    imageId);

//                return RedirectToAction("Index");
//            }

//            var categories = this.categories.GetAll().To<CategoryViewModel>().ToList();
//            ViewBag.CategoryId = new SelectList(categories, "Id", "Name");
//            return View(model);
//        }

//        // GET: Admin/Reviews/Delete/5
//        public ActionResult Delete(int? id)
//        {
//            if (id == null)
//            {
//                return new HttpStatusCodeResult(HttpStatusCode.BadRequest);
//            }

//            Review review = this.reviews.GetById((int)id);
//            if (review == null)
//            {
//                return HttpNotFound();
//            }

//            var viewModel = AutoMapperConfig.Configuration.CreateMapper().Map<ReviewViewModel>(review);
//            return View(viewModel);
//        }

//        // POST: Admin/Reviews/Delete/5
//        [HttpPost, ActionName("Delete")]
//        [ValidateAntiForgeryToken]
//        public ActionResult DeleteConfirmed(int id)
//        {
//            this.reviews.Delete(id);
//            return RedirectToAction("Index");
//        }
//    }
//}