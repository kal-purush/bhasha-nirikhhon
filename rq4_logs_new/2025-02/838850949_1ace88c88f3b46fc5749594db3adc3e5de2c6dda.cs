using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using ModelLibrary.Dto;
using Newtonsoft.Json;
using System.IdentityModel.Tokens.Jwt;
using Web.Service.IService;

namespace Web.Controllers
{
    public class CartController : Controller
    {
        private readonly ICartService _cartService;
        public CartController(ICartService cartService)
        {
            _cartService = cartService;
        }

        [Authorize]
        public async Task<IActionResult> CartIndex()
        {
            try
            {
                var userId = User.Claims.Where(u => u.Type == JwtRegisteredClaimNames.Sub)?.FirstOrDefault()?.Value;
                if (string.IsNullOrEmpty(userId))
                {
                    throw new Exception("Error: the user cannot be found");
                }
                var response = await _cartService.GetCartAsync(userId);
                if (response==null || !response.IsSuccess)
                {
                    throw new Exception("Error: the shopping cart cannot be found");
                }
                CartDto cartDto = JsonConvert.DeserializeObject<CartDto>(Convert.ToString(response.Result));
                return View(cartDto);
            }
            catch (Exception e)
            {
                TempData["error"] = e.Message;
                return RedirectToAction(nameof(HomeController.Index), nameof(HomeController).Replace("Controller", ""));
            }
        }
    }
}