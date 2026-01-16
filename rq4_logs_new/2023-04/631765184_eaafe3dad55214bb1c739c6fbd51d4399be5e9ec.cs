using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using OrderAPI.Models;
using OrderAPI.repositories.IRepon;
using OrderAPI.repositories.Repon;
using OrderAPI.ViewModel.Order;

namespace OrderAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    
    public class OrderController : ControllerBase
    {
        private readonly IOrderRepon _orderRepon;
        public OrderController(IOrderRepon orderRepon)
        {
           _orderRepon = orderRepon;
        }
        [HttpGet]
        public async Task<IActionResult> GetById(string PhoneNumber)
        {
            Customer customer = new Customer();
            using (var httpClient = new HttpClient())
            {
                var response = await httpClient.GetAsync($"https://localhost:7283/api/Customer/GetBySdt?sdt={PhoneNumber}");
                customer = await response.Content.ReadAsAsync<Customer>();
            }

            return Ok(customer);
        }
        [HttpPost]
        //[Authorize]
        public async Task<IActionResult> Create( CreateOrder createOrder)
        {
            int result = await _orderRepon.Create(createOrder);
            if(result == 1) { return Ok("chưa có người dùng này mong bạn điền đầy đủ thông ti"); };
            if (result == 2) { return Ok("lỗi không thêm được người dùng"); };
            if (result == 3) { return Ok("thêm thanh công"); };
            if (result == 4) { return Ok("lỗi không thêm được hoán đơn"); };
            return Ok("chưa biết ai hơn ai đâu");
        }
    }
}