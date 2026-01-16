using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using DFBangazon.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace DFBangazon.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class OrdersController : ControllerBase
    {
        private readonly IConfiguration _config;

        public OrdersController(IConfiguration config)
        {
            _config = config;
        }

        public IDbConnection Connection
        {
            get
            {
                return new SqlConnection(_config.GetConnectionString("DefaultConnection"));
            }
        }

        // GET api/ordes?somethinghere
        [HttpGet]
        public async Task<IActionResult> Get(string completed)
        {
            using (IDbConnection conn = Connection)
            {
                string sql = "SELECT * FROM Orders";

                if(completed == "false")
                {
                    sql += $" WHERE PaymentTypeId is null";
                }

                var fullOrders = await conn.QueryAsync<Orders>(sql);
                return Ok(fullOrders);
            }

        }

        // GET api/orders/5
        [HttpGet("{id}", Name = "GetOrder")]
        public async Task<IActionResult> Get([FromRoute] int id)
        {
            using (IDbConnection conn = Connection)
            {
                string sql = $"SELECT * FROM Orders WHERE Id = {id}";

                var theSingleOrder = (await conn.QueryAsync<Orders>(sql)).Single();
                return Ok(theSingleOrder);
            }
        }

        // POST api/values
        [HttpPost]
        public async Task<IActionResult> Post([FromBody] Orders orders)
        {
            string sql = $@"INSERT INTO Orders
            (OrderDate, CustomerId, PaymentTypeId)
            VALUES
            ('{orders.OrderDate}', '{orders.CustomerId}', '{orders.PaymentTypeId}');
            select MAX(Id) from Orders";

            using (IDbConnection conn = Connection)
            {
                var newOrdersId = (await conn.QueryAsync<int>(sql)).Single();
                orders.Id = newOrdersId;
                return CreatedAtRoute("GetOrder", new { id = newOrdersId }, orders);
            }

        }

        // PUT api/values/5
        [HttpPut("{id}")]
        public async Task<IActionResult> Put([FromRoute] int id, [FromBody] Orders orders)
        {
            string sql = $@"
            UPDATE Orders
            SET OrderDate = '{orders.OrderDate}',
                CustomerId = '{orders.CustomerId}',
                PaymentTypeId = '{orders.PaymentTypeId}'
            WHERE Id = {id}";

            try
            {
                using (IDbConnection conn = Connection)
                {
                    int rowsAffected = await conn.ExecuteAsync(sql);
                    if (rowsAffected > 0)
                    {
                        return new StatusCodeResult(StatusCodes.Status204NoContent);
                    }
                    throw new Exception("No rows affected");
                }
            }
            catch (Exception)
            {
                if (!OrderExists(id))
                {
                    return NotFound();
                }
                else
                {
                    throw;
                }
            }
        }

        // DELETE api/values/5
        [HttpDelete("{id}")]
        public async Task<IActionResult> Delete([FromRoute] int id)
        {
            string sql = $@"DELETE FROM Orders WHERE Id = {id}";

            using (IDbConnection conn = Connection)
            {
                int rowsAffected = await conn.ExecuteAsync(sql);
                if (rowsAffected > 0)
                {
                    return new StatusCodeResult(StatusCodes.Status204NoContent);
                }
                throw new Exception("No rows affected");
            }

        }

        private bool OrderExists(int id)
        {
            string sql = $"SELECT Id, OrderDate, CustomerId, PaymentTypeId FROM Orders WHERE Id = {id}";
            using (IDbConnection conn = Connection)
            {
                return conn.Query<Orders>(sql).Count() > 0;
            }
        }
    }
}