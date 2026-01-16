using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BourbonAndBurial.Data;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace BourbonAndBurial.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class OrderProductController : ControllerBase
    {
        readonly OrderProductRepository _orderProductRepository;

        public OrderProductController()
        {
            _orderProductRepository = new OrderProductRepository();
        }
        [HttpGet]
        public ActionResult GetAll(int CustomerId)
        {
            var orders = _orderProductRepository.GetAll(CustomerId);
            return Ok(orders);
        }
    }
}