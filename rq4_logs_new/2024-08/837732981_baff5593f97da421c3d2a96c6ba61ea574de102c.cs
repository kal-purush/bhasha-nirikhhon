using SampleEcommerceWebsite.DataAccess.Repository.IRepository;
using Microsoft.AspNetCore.Mvc;
using SampleEcommerceWebsite.Utility;
using System.Security.Claims;

namespace SampleEcommerceWebsite.ViewComponents
{
    public class ShoppingCartViewComponent : ViewComponent
    {
        //using dependency injection
        private readonly IUnitOfWork _unitOfWork;
        public ShoppingCartViewComponent(IUnitOfWork unitOfWork)
        {
            _unitOfWork = unitOfWork;
        }

        //handle backend functionality of shoppingcartviewcomponent
        public async Task<IViewComponentResult> InvokeAsync()
        {
            var claimsIdentity = (ClaimsIdentity)User.Identity;
            var claim = claimsIdentity.FindFirst(ClaimTypes.NameIdentifier);

            if (claim != null)
            {
                if (HttpContext.Session.GetInt32(StaticDetails.SessionCart) == null)
                {
                    HttpContext.Session.SetInt32(StaticDetails.SessionCart,
                    _unitOfWork.ShoppingCart.GetAll(u => u.ApplicationUserId == claim.Value).Count());
                }
                
                return View(HttpContext.Session.GetInt32(StaticDetails.SessionCart));
            }
            else
            {
                HttpContext.Session.Clear();
                return View(0);
            }

        }
    }
}