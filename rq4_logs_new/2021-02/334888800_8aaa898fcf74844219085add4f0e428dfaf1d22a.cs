using cuisin.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace cuisin.Controllers
{
    public class CustomerController : ApiController
    {
        // GET api/<controller>


        // GET api/<controller>/5
        public IHttpActionResult Get(string mail, string password)
        {
            try
            {
                Customer Customer = new Customer();
                List<Customer> Customer_Islogged = Customer.CheckIfLog(mail, password);
                return Ok(Customer_Islogged);
            }
            catch (Exception e)
            {
                return Content(HttpStatusCode.BadRequest, e);


            }
        }

        // POST api/<controller>
        public IHttpActionResult Post([FromBody]Customer customer)
        {
            try
            {
                int count = customer.Insert();
                return Created(new Uri(Request.RequestUri.AbsoluteUri + customer.Id), count);

            }
            catch (Exception e)
            {

                return BadRequest(e.Message);
            }
        }

        // PUT api/<controller>/5
        public void Put(int id, [FromBody]string value)
        {
        }

        // DELETE api/<controller>/5
        public void Delete(int id)
        {
        }
    }
}