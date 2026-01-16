using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Dapper;
using DFBangazon.Models;
using Microsoft.AspNetCore.Http;

namespace DFBangazon.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CustomerController : ControllerBase
    {
        private readonly IConfiguration _config;

        public CustomerController(IConfiguration config)
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
        // GET api/values
        [HttpGet]
        public async Task<IActionResult> Get()
        {
            using (IDbConnection conn = Connection)
            {
                string sql = "SELECT * FROM Customer";

                var allCustomers = (await conn.QueryAsync<Customer>(sql));
                return Ok(allCustomers);
            }
        }

        // GET api/values/5
        [HttpGet("{id}")]
        public async Task<IActionResult> Get([FromRoute] int id)
        {
            using (IDbConnection conn = Connection)
            {
                string sql = $"SELECT * FROM Customer WHERE Id = {id}";

                var theSingleCustomer = (await conn.QueryAsync<Customer>(sql)).Single();
                return Ok(theSingleCustomer);
            }
        }

        // POST api/values
        [HttpPost]
        public async Task<IActionResult> Post([FromBody] Customer customer)
        {
            string sql = $@"INSERT INTO Customer
            (FirstName, LastName, AccountCreated, LastLogin)
            VALUES
            ('{customer.FirstName}', '{customer.LastName}', '{customer.AccountCreated}', '{customer.LastLogin}')
            select MAX(Id) from Customer";

            using (IDbConnection conn = Connection)
            {
                var newCustomerId = (await conn.QueryAsync<int>(sql)).Single();
                customer.Id = newCustomerId;
                return CreatedAtRoute("GetCustomer", new { id = newCustomerId }, customer);
            }
        }

        // PUT api/values/5
        [HttpPut("{id}")]
        public async Task<IActionResult> Put([FromRoute] int id, [FromBody] Customer customer)
        {
            string sql = $@"
            UPDATE Customer
            SET FirstName = '{customer.FirstName}',
                LastName = '{customer.LastName}',
                AccountCreated = '{customer.AccountCreated}',
                LastLogin = '{customer.LastLogin}'
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
                if (!ExerciseExists(id))
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
            string sql = $@"DELETE FROM Customer WHERE Id = {id}";

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

        private bool ExerciseExists(int id)
        {
            string sql = $"SELECT Id, FirstName, LastName, AccountCreated, LastLogin FROM Customer WHERE Id = {id}";
            using (IDbConnection conn = Connection)
            {
                return conn.Query<Customer>(sql).Count() > 0;
            }
        }
    }
}
