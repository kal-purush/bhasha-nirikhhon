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
using Microsoft.EntityFrameworkCore;
using NToastNotify;

namespace CM.Web.Areas.BarCrawler.Controllers
{
    [Area("BarCrawler")]
    public class RatingsController : Controller
    {
        private readonly IRatingServices _ratingServices;
        private readonly IRatingViewMapper _ratingViewMapper;
        private readonly IToastNotification _toastNotification;

        public RatingsController(IRatingServices ratingServices,
                                  IRatingViewMapper ratingViewMapper,
                                  IToastNotification toastNotification)
        {
            this._ratingServices = ratingServices;
            this._ratingViewMapper = ratingViewMapper;
            this._toastNotification = toastNotification;
        }

        // POST: CommentController/Create
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> RateCocktail(RatingViewModel model)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    model.UserId = Guid.Parse(User.FindFirst(ClaimTypes.NameIdentifier).Value);
                    var dto = _ratingViewMapper.CreateCocktailRatingDTO(model);
                    dto = await _ratingServices.RateCocktailAsync(dto);

                    _toastNotification.AddWarningToastMessage($"You rated this cocktail with {model.Score}");
                    return RedirectToAction("Details", "Cocktails", new { area = "", Id = model.EntityId });
                }
                catch (DbUpdateException)
                {
                    return RedirectToAction(nameof(UpdateCocktailRating), model);
                }
                catch (Exception ex)
                {
                    _toastNotification.AddErrorToastMessage(ex.Message);
                    return RedirectToAction("Details", "Cocktails", new { area = "", Id = model.EntityId });
                }
            }

            foreach (var item in ModelState.Values)
            {
                foreach (var error in item.Errors)
                {
                    _toastNotification.AddWarningToastMessage(error.ErrorMessage);
                }
            }
            return RedirectToAction("Details", "Cocktails", new { area = "", Id = model.EntityId });
        }

        public async Task<ActionResult> UpdateCocktailRating(RatingViewModel model)
        {
            model.UserId = Guid.Parse(User.FindFirst(ClaimTypes.NameIdentifier).Value);
            var dto = _ratingViewMapper.CreateCocktailRatingDTO(model);
            dto = await _ratingServices.EditCocktailRatingAsync(dto);

            _toastNotification.AddWarningToastMessage($"You updated your rating for this cocktail to {model.Score}");
            return RedirectToAction("Details", "Cocktails", new { area = "", Id = model.EntityId });
        }
    }
}