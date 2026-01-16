//coded by jenn. This controller provides get, put, post and delete methods for employee.
using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using System.Data;
using System.Data.SqlClient;
using Dapper;
using DFBangazon.Models;
using Microsoft.AspNetCore.Http;

namespace DFBangazon.Controllers //namespace of controller
{
    [Route("api/[controller]")] //route to API 
    [ApiController]
    public class EmployeetController : ControllerBase //gives product controller the inheritance of controller base
    {
        private readonly IConfiguration _config; //setting the configuration value to a private readonly _config

        public EmployeeController(IConfiguration config) //a public constructor to set the IConfiguration
        {
            _config = config;
        }

        public IDbConnection Connection
        {
            get
            {
                return new SqlConnection(_config.GetConnectionString("DefaultConnection")); //sets default connection from app settings in Connection
            }
        }

        // GET api/employee?firstName="FristName"
        [HttpGet]
        public async Task<IActionResult> Get(string firstName)  //defining the git request gets employee by first name
        {
            using (IDbConnection conn = Connection) //setting the connectin to conn
            {
                string sql = "SELECT * FROM Employee"; //starting the sql command, selecting all from employee

                if (firstName != null) //selecting from name of employee
                {
                    sql += $" WHERE FirstName='{firstName}'";
                }

                var fullProducts = await conn.QueryAsync<Product>(sql);  //exposes the enumerator
                return Ok(fullProducts); //returns selected employee
            }

        }

        // GET api/values/5
        [HttpGet("{id}", Name = "GetEmployee")]
        public async Task<IActionResult> Get([FromRoute] int id) //gets a single employee by id
        {
            using (IDbConnection conn = Connection)  //setting the connectin to conn
            {
                string sql = $"SELECT * FROM Employee WHERE Id = {id}"; //sql select by id

                var singleProduct = (await conn.QueryAsync<Product>(sql)).Single(); //exposes the enumerator 
                return Ok(singleProduct); //returns the selected product
            }
        }

        // POST api/values
        [HttpPost]
        public async Task<IActionResult> Post([FromBody] Employee employee) //posts a employee
        {
            string sql = $@"INSERT INTO Employee 
            (FirstName, LastName, IsSupervisor, DepartmentId)
            VALUES
            ('{employee.FirstName}', '{employee.LastName}', '{employee.IsSupervisor}', '{employee.DepartmentId}');
            select MAX(Id) from Employee"; //returns the last made employee

            using (IDbConnection conn = Connection) //setting connection to conn
            {
                var newEmployeeId = (await conn.QueryAsync<int>(sql)).Single(); //exposes the enumerator and sets to newEmployee
                employee.Id = newEmployeeId;
                return CreatedAtRoute("GetEmployee", new { id = newEmployeeId }, employee); //posts and returns the employee you just added
            }

        }

        // PUT api/values/5
        [HttpPut("{id}")]
        public async Task<IActionResult> Put([FromRoute] int id, [FromBody] Product product) //edits an existing product
        {
            string sql = $@" 
            UPDATE Product
            SET Price = '{product.Price}',
                Title = '{product.Title}', 
                ProdDesc = '{product.ProdDesc}',
                Quantity = '{product.Quantity}',
                ProductTypeId = '{product.ProductTypeId}',
                SellerId = '{product.SellerId}'
            WHERE Id = {id}"; //edits product by id

            try
            {
                using (IDbConnection conn = Connection) //sets connection to conn
                {
                    int rowsAffected = await conn.ExecuteAsync(sql); //exposes the enumerator and sets it to rowsAffected
                    if (rowsAffected > 0)
                    {
                        return new StatusCodeResult(StatusCodes.Status204NoContent); //retuns status code 204, success but does not retun product changed
                    }
                    throw new Exception("No rows affected"); //gives alert that no edit took place
                }
            }
            catch (Exception)
            {
                if (!ProductExists(id)) //if product id does not exist,
                {
                    return NotFound(); //return a statement "not found"
                }
                else
                {
                    throw; //otherwise, throw default error statement
                }
            }
        }

        // DELETE api/values/5
        [HttpDelete("{id}")]
        public async Task<IActionResult> Delete([FromRoute] int id) //deletes a product by id
        {
            string sql = $@"DELETE FROM Product WHERE Id = {id}"; //sql command that deletes product by id

            using (IDbConnection conn = Connection)
            {
                int rowsAffected = await conn.ExecuteAsync(sql); //exposes the enumerator and sets rowsAffected
                if (rowsAffected > 0)
                {
                    return new StatusCodeResult(StatusCodes.Status204NoContent); //if sucess, returns sataus 204, with no comment
                }
                throw new Exception("No rows affected"); //otherwise, returns error statement
            }

        }

        private bool ProductExists(int id) //checks if product exists by id
        {
            string sql = $"SELECT Id, Price, Title, ProdDesc, Quantity, ProductTypeId, SellerId FROM Product WHERE Id = {id}";
            using (IDbConnection conn = Connection)
            {
                return conn.Query<Product>(sql).Count() > 0; //returns product by id
            }
        }
    }
}