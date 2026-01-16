using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Dapper;
using SQLitePCL;
using System.Data.SQLite;
using Microsoft.Extensions.Configuration;
using freshop.Services;
using freshop.Models;
using freshop.Repositories;

namespace freshop.Controllers
{
    [Route("api/[controller]")]
    public class OrderController : Controller
    {
        private readonly OrderService orderService;

        public OrderController(IConfiguration configuration)
        {
            var connectionString = configuration.GetConnectionString("ConnectionString");
            this.orderService = new OrderService(new CustomerRepository(connectionString), new OrderValueRepository(connectionString), new OrdersRepository(connectionString), new OrderItemsRepository(connectionString));
        }

        [HttpGet("{guid}")]
        [ProducesResponseType(typeof(Order), StatusCodes.Status200OK)]
        [ProducesResponseTypeAttribute(StatusCodes.Status404NotFound)]
        public IActionResult Get(string guid)
        {
            return Ok(this.orderService.Get(guid));
        }
    }
}