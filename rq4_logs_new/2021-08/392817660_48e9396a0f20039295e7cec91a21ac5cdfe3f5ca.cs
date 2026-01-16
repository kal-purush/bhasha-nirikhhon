using AutoMapper;
using BestUzdNew.Api.Dtos.Input;
using BestUzdNew.Domain.Entities;
using BestUzdNew.Logic;
using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace BestUzdNew.Api.Controllers
{
    [Route("api/order")]
    [ApiController]
    public class OrderController : ControllerBase
    {
        private readonly IMapper _mapper;
        private readonly IOrderService _orderService;

        public OrderController(IMapper mapper, IOrderService orderService)
        {
            _mapper = mapper;
            _orderService = orderService;
        }


        [HttpPost]
        public async Task<IActionResult> CreateOrder(OrderInDto orderInDto)
        {
            var order = _mapper.Map<OrderInDto, Order>(orderInDto);
            await _orderService.CreateOrderAsync(order);

            return Ok();
        }

        [HttpGet]
        public async Task<IActionResult> GetOrders()
        {
            var orders = await _orderService.GetOrdersAsync();
            var ordersDto = _mapper.Map<IEnumerable<Order>, IEnumerable<OrderInDto>>(orders);

            return Ok(ordersDto);
        }

    }
}