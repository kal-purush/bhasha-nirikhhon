using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ToDoList.Model;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace ToDoList.Controllers
{
    [Route("api/FormData")]
    [ApiController]
    public class FormController : ControllerBase
    {
        // GET all departments and customers
        [HttpGet]
        public IEnumerable<Form> Get()
        {
            Form Result = new Form();
            Result.GetAllDepartmentNames();
            Result.GetAllCustomerNames();
            Result.GetContractManagerNames();
            return new Form[] { Result };
        }

        // GET all Contract managers for a certain department
        [HttpGet("{DepartmentName}")]
        public IEnumerable<Form> Get(string DepartmentName)
        {
            Form Result = new Form();
            Result.GetContractManagerNames();
            return new Form[] { Result };
        }

        // POST new site
        //[HttpPost]
        //public Site Post([FromBody] Site site)
        //{
        //    site.CreateNewSite();
        //    return site;
        //}

        //// PUT api/<DepartmentsController>/5
        //[HttpPut("{id}")]
        //public void Put(int id, [FromBody] string value)
        //{
        //}

        //// DELETE api/<DepartmentsController>/5
        //[HttpDelete("{id}")]
        //public void Delete(int id)
        //{
        //}
    }
}