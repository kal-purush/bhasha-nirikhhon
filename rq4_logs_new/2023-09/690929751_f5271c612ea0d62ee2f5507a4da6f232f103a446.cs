using Mango.Web.Models;
using Mango.Web.Service.IService;
using Mango.Web.Utility;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using System.IdentityModel.Tokens.Jwt;

namespace Mango.Web.Controllers
{
    public class OrderController : Controller
    {
        private readonly IOrderService _orderService;
        public OrderController(IOrderService orderService)
        {
            _orderService = orderService;
        }
        public IActionResult OrderIndex()
        {
            return View();
        }

        [HttpGet]
        public IActionResult GetAll()
        {
            IEnumerable<OrderDTO> orderlist;
            string userId = "";
            if(!User.IsInRole(StaticDetails.RoleAdmin))
            {
                userId = User.Claims.Where(u => u.Type == JwtRegisteredClaimNames.Sub)?.FirstOrDefault()?.Value;
            }
            ResponseType response = _orderService.GetAllOrder(userId).GetAwaiter().GetResult();
            if(response != null)
            {
                orderlist = JsonConvert.DeserializeObject<List<OrderDTO>>(Convert.ToString(response.Result));
            }
            else
            {
                orderlist = new List<OrderDTO>();
            }
            return Json(new { data = orderlist });
        }

		[HttpGet]
		public async Task<IActionResult> OrderDetail(int orderId)
		{
			OrderDTO orderdetail = new OrderDTO();
			string userId = "";
			if (!User.IsInRole(StaticDetails.RoleAdmin))
			{
				userId = User.Claims.Where(u => u.Type == JwtRegisteredClaimNames.Sub)?.FirstOrDefault()?.Value;
			}
			ResponseType response = await _orderService.GetOrder(orderId);
			if (response != null)
			{
				orderdetail = JsonConvert.DeserializeObject<OrderDTO>(Convert.ToString(response.Result));
			}
            if(!(User.IsInRole(StaticDetails.RoleAdmin)) && (userId!= orderdetail?.UserId))
            {
                return NotFound();
            }
			return View(orderdetail);
		}

        [HttpPost("OrderReadyForPickup")]
        public async Task<IActionResult> OrderReadyForPickup(int orderId)
        {
            var response = await _orderService.UpdateOrderStatus(orderId, StaticDetails.STATUS_READYFORPICKUP);
            if (response != null && response.IsSuccess)
            {
                TempData["success"] = "Status updated successfully";
                return RedirectToAction(nameof(OrderDetail), new { orderId = orderId });
            }
            return View();
        }

        [HttpPost("CompleteOrder")]
        public async Task<IActionResult> CompleteOrder(int orderId)
        {
            var response = await _orderService.UpdateOrderStatus(orderId, StaticDetails.STATUS_COMPLETED);
            if (response != null && response.IsSuccess)
            {
                TempData["success"] = "Status updated successfully";
                return RedirectToAction(nameof(OrderDetail), new { orderId = orderId });
            }
            return View();
        }

        [HttpPost("CancelOrder")]
        public async Task<IActionResult> CancelOrder(int orderId)
        {
            var response = await _orderService.UpdateOrderStatus(orderId, StaticDetails.STATUS_CANCELLED);
            if (response != null && response.IsSuccess)
            {
                TempData["success"] = "Status updated successfully";
                return RedirectToAction(nameof(OrderDetail), new { orderId = orderId });
            }
            return View();
        }

    }
}