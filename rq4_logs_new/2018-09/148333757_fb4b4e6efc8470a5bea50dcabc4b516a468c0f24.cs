// Author: Sathvik Reddy
// Purpose: An Employee should be assigned a computer. Once that computer is assigned, that particular computer should not be given out to anyone else
// Methods: This controller utilizes Get, Post, Put and Patch methods

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
    public class ComputerController : ControllerBase
    {
        private readonly IConfiguration _config;

        public ComputerController(IConfiguration config)
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

        // GET api/(alldata)
        [HttpGet]
        public async Task<IActionResult> Get()
        {
            using (IDbConnection conn = Connection)
            {
                string sql = "SELECT * FROM Computer";

                var fullComputer = await conn.QueryAsync<Computer>(sql);
                return Ok(fullComputer);
            }

        }

        // GET api/values/id
        [HttpGet("{id}")]
        public async Task<IActionResult> Get([FromRoute] int id)
        {
            using (IDbConnection conn = Connection)
            {
                string sql = $"SELECT * FROM Computer WHERE Id = {id}";

                var theSingleComputer = (await conn.QueryAsync<Computer>(sql)).Single();
                return Ok(theSingleComputer);
            }
        }

        // POST api/values
        [HttpPost]
        public async Task<IActionResult> Post([FromBody] Computer computer)
        {
            string sql = $@"INSERT INTO Computer
            (PurchaseDate, Model, DecommissionDate, Condition)
            VALUES
            ('{computer.PurchaseDate}', '{computer.Model}', '{computer.DecommissionDate}', '{computer.Condition}');
            select MAX(Id) from Computer";

            using (IDbConnection conn = Connection)
            {
                var newComputerId = (await conn.QueryAsync<int>(sql)).Single();
                computer.Id = newComputerId;
                return CreatedAtRoute("GetComputer", new { id = newComputerId }, computer);
            }

        }

        // PUT api/values/5
        [HttpPut("{id}")]
        public async Task<IActionResult> Put([FromRoute] int id, [FromBody] Computer computer)
        {
            string sql = $@"
            UPDATE Computer
            SET PurchaseDate = '{computer.PurchaseDate}',
                Model = '{computer.Model}',
                DecommissionDate = '{computer.DecommissionDate}',
                Condition = '{computer.Condition}'
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
                if (!ComputerExists(id))
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
            string sql = $@"DELETE FROM Computer WHERE Id = {id}";

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

        private bool ComputerExists(int id)
        {
            string sql = $"SELECT Id, PurchaseDate, Model, DecommissionDate, Condition FROM Computer WHERE Id = {id}";
            using (IDbConnection conn = Connection)
            {
                return conn.Query<Computer>(sql).Count() > 0;
            }
        }
    }
}