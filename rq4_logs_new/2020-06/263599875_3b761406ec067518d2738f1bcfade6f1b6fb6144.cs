using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Threading.Tasks;
using CM.Models;
using CM.Services.Contracts;
using CM.Web.Areas.BarCrawler.Models;
using CM.Web.Providers.Contracts;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using NToastNotify;

namespace CM.Web.Areas.BarCrawler.Controllers
{
    [Area("BarCrawler")]
    public class CommentsController : Controller
    {
        private readonly ICommentServices _commentServices;
        private readonly ICommentViewMapper _commentViewMapper;
        private readonly IToastNotification _toastNotification;

        public CommentsController(ICommentServices commentServices,
                                  ICommentViewMapper commentViewMapper,
                                  IToastNotification toastNotification)
        {
            this._commentServices = commentServices;
            this._commentViewMapper = commentViewMapper;
            this._toastNotification = toastNotification;
        }

        // POST: CommentController/Create
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> Create(CommentViewModel model)
        {
            try
            {
                model.UserId = Guid.Parse(User.FindFirst(ClaimTypes.NameIdentifier).Value);
                var dto = _commentViewMapper.CreateCocktailCommentDTO(model);
                dto = await _commentServices.AddCocktailCommentAsync(dto);

                return RedirectToAction("Details", "Cocktails", new { Id = model.EntityId});
            }
            catch (Exception ex)
            {
                _toastNotification.AddErrorToastMessage(ex.Message);
                return RedirectToAction("Details", "Cocktails", new { area = "", Id = model.EntityId });
            }
        }

        // GET: CommentController/Edit/5
        public ActionResult Edit(int id)
        {
            return View();
        }

        // POST: CommentController/Edit/5
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Edit(int id, IFormCollection collection)
        {
            try
            {
                return RedirectToAction(nameof(Index));
            }
            catch
            {
                return View();
            }
        }

        // GET: CommentController/Delete/5
        public ActionResult Delete(int id)
        {
            return View();
        }

        // POST: CommentController/Delete/5
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Delete(int id, IFormCollection collection)
        {
            try
            {
                return RedirectToAction(nameof(Index));
            }
            catch
            {
                return View();
            }
        }
    }
}