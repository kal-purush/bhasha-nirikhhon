namespace BlogSystem.Web.Areas.Administration.Controllers
{
    using System;
    using System.Linq;
    using System.Web.Mvc;

    using AutoMapper.QueryableExtensions;

    using BlogSystem.Data.Common.Repository;
    using BlogSystem.Data.Models;
    using BlogSystem.Web.Areas.Administration.ViewModels.BlogPost;
    using BlogSystem.Web.Areas.Administration.ViewModels.Comments;

    public class CommentsController : AdminBaseController
    {
        private readonly IDeletableEntityRepository<Comment> comments;
        private readonly IRepository<BlogPost> blogPosts;

        public CommentsController(IDeletableEntityRepository<Comment> comments, IRepository<BlogPost> blogPosts)
        {
            this.comments = comments;
            this.blogPosts = blogPosts;
        }

        // GET: Administration/Comments
        public ActionResult Index(int id, int page = 1, int perPage = 0)
        {
            perPage = perPage > 0 ? perPage : int.Parse(this.ViewBag.Settings.Get["Comments Per Page"]);
            var blogPostDb = this.blogPosts.All()
                .Where(x => x.Id == id)
                .Project()
                .To<BlogPostSimpleViewModel>()
                .FirstOrDefault();

            if (blogPostDb == null)
            {
                return this.HttpNotFound("No such Blog Post");
            }

            var commentsDb = this.comments.All()
                .Where(x => x.BlogPost.Id == blogPostDb.Id)
                .OrderBy(x => x.CreatedOn)
                .Skip(perPage * (page - 1))
                .Take(perPage)
                .Project()
                .To<CommentSimpleViewModel>()
                .ToArray();

            var model = new CommentsIndexViewModel
            {
                BlogPost = blogPostDb,
                Comments = commentsDb,
                CurrentPage = page,
                TotalPages = (int)Math.Ceiling(this.comments.All().Count(x => x.BlogPostId == id) / (decimal)perPage)
            };
            return this.View(model);
        }

        [HttpGet]
        public ActionResult Edit(int id)
        {
            var model = this.comments.All().Where(x => x.Id == id).Project().To<CommentInputModel>().FirstOrDefault();
            if (model == null)
            {
                return this.HttpNotFound("No such Comment");
            }

            return this.View(model);
        }

        [HttpPost]
        public ActionResult Edit(CommentInputModel model)
        {
            if (ModelState.IsValid)
            {
                var commentDb = this.comments.GetById(model.Id);
                commentDb.Content = model.Content;
                commentDb.IsVisible = model.IsVisible;
                commentDb.ReasonToHide = model.ReasonToHide;

                this.comments.Update(commentDb);
                this.comments.SaveChanges();

                return this.RedirectToAction("Index", new { id = model.BlogPostId });
            }

            return this.View(model);
        }
    }
}