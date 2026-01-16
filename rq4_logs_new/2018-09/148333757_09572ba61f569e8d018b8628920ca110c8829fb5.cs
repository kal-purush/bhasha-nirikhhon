// Author: Sathvik Reddy
// Purpose: Controller -  a customer can have many Payment Types; however, if a cutomer wants to remove a Payment Type, then it will be removed from their view. Customer payment data remains in Bangazon's database, regardless of whether it is active or not.
// Methods: This controller utilizes Get, Post and Put methods

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using System.Data;
using System.Data.SqlClient;
using Dapper;
using DFBangazon.Models;
using Microsoft.AspNetCore.Http;

namespace DFBangazon.Models
{
    [Route("api/[controller]")]
    [ApiController]
    public class PaymentTypeController : ControllerBase
    {
        private readonly IConfiguration _config;

        public PaymentTypeController(IConfiguration config)
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

        // GET * | queries PaymentType table and returns all data
        [HttpGet]
        public async Task<IActionResult> Get()
        {
            using (IDbConnection conn = Connection)
            {
                string sql = "SELECT * FROM PaymentType";

                var fullPaymentType = await conn.QueryAsync<PaymentType>(sql);
                return Ok(fullPaymentType);
            }

        }

        // GET id | queries PaymentType table, but only returns the specified id
        [HttpGet("{id}")]
        public async Task<IActionResult> Get([FromRoute] int id)
        {
            using (IDbConnection conn = Connection)
            {
                string sql = $"SELECT * FROM PaymentType WHERE Id = {id}";

                var theSinglePaymentType = (await conn.QueryAsync<PaymentType>(sql)).Single();
                return Ok(theSinglePaymentType);
            }
        }

        // POST | adds data to PaymentType table
        [HttpPost]
        public async Task<IActionResult> Post([FromBody] PaymentType paymentType)
        {
            string sql = $@"INSERT INTO PaymentType
            (CustomerId, AccountNo, AccType, Nickname, IsActive)
            VALUES
            ('{paymentType.CustomerId}', '{paymentType.AccountNo}', '{paymentType.AccType}', '{paymentType.Nickname}', '{paymentType.IsActive}');
            select MAX(Id) from PaymentType";

            using (IDbConnection conn = Connection)
            {
                var newPaymentTypeId = (await conn.QueryAsync<int>(sql)).Single();
                paymentType.Id = newPaymentTypeId;
                return CreatedAtRoute("GetPaymentType", new { id = newPaymentTypeId }, paymentType);
            }

        }

        // PUT | edits data in PaymentType table based on id
        [HttpPut("{id}")]
        public async Task<IActionResult> Put([FromRoute] int id, [FromBody] PaymentType paymentType)
        {
            string sql = $@"
            UPDATE PaymentType
            SET AccountNo = '{paymentType.AccountNo}',
                AccType = '{paymentType.AccType}',
                Nickname = '{paymentType.Nickname}',
                IsActive = '{paymentType.IsActive}'
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
                if (!PaymentTypeExists(id))
                {
                    return NotFound();
                }
                else
                {
                    throw;
                }
            }
        }

        // DELETE | deletes data in PaymentType table, which is specified by id
        [HttpDelete("{id}")]
        public async Task<IActionResult> Delete([FromRoute] int id)
        {
            string sql = $@"DELETE FROM PaymentType WHERE Id = {id}";

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

        // Queries all data in PaymentType table
        private bool PaymentTypeExists(int id)
        {
            string sql = $"SELECT Id, AccountNo, AccType, Nickname, IsActive FROM PaymentType WHERE Id = {id}";
            using (IDbConnection conn = Connection)
            {
                return conn.Query<PaymentType>(sql).Count() > 0;
            }
        }
    }
}