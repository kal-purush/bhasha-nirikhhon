using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using System.Data;
using System.Data.SqlClient;
using Dapper;
using Product.Models;
using Microsoft.AspNetCore.Http;

namespace DFBangazon.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProductController : ControllerBase
    {
        private readonly IConfiguration _config;

        public ProductController(IConfiguration config)
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

        // GET api/product?name="productTitle"
        [HttpGet]
        public async Task<IActionResult> Get(string title)
        {
            using (IDbConnection conn = Connection)
            {
                string sql = "SELECT * FROM Product";

                if (title != null)
                {
                    sql += $" WHERE Title='{title}'";
                }

                var fullProducts = await conn.QueryAsync<Product>(sql);
                return Ok(fullProducts);
            }

        }

        // GET api/values/5
        [HttpGet("{id}", Name = "GetProduct")]
        public async Task<IActionResult> Get([FromRoute] int id)
        {
            using (IDbConnection conn = Connection)
            {
                string sql = $"SELECT * FROM Product WHERE Id = {id}";

                var singleProduct = (await conn.QueryAsync<Product>(sql)).Single();
                return Ok(singleProduct);
            }
        }

        // POST api/values
        [HttpPost]
        public async Task<IActionResult> Post([FromBody] Product product)
        {
            string sql = $@"INSERT INTO Product
            (Price, Title, ProductDesc, Quantity, ProductTypeId, CuostomerId)
            VALUES
            ('{product.Price}', '{product.Title}', '{product.Quantity}', '{product.ProductTypeId}', '{product.CustomerId}',);
            select MAX(Id) from Product";

            using (IDbConnection conn = Connection)
            {
                var newProductId = (await conn.QueryAsync<int>(sql)).Single();
                product.Id = newProductId;
                return CreatedAtRoute("GetProduct", new { id = newProductId }, product);
            }

        }

        // PUT api/values/5
        [HttpPut("{id}")]
        public async Task<IActionResult> Put([FromRoute] int id, [FromBody] Product product)
        {
            string sql = $@"
            UPDATE Product
            SET Price = '{product.Price}',
                Title = '{product.Title}',
                ProdcutDesc = '{product.ProductDesc}',
                Quantity = '{product.Quantity}',
                ProductTypeId = '{product.ProductTypeId}',
                CustomerId = '{product.CustomerId}'
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
                if (!ProductExists(id))
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
            string sql = $@"DELETE FROM Product WHERE Id = {id}";

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

        private bool ProductExists(int id)
        {
            string sql = $"SELECT Id, Price, Title, ProductDesc, Quantity, ProductTypeId, CustomerId FROM Product WHERE Id = {id}";
            using (IDbConnection conn = Connection)
            {
                return conn.Query<Product>(sql).Count() > 0;
            }
        }
    }
}