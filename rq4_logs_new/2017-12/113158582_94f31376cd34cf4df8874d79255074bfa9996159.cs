using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace MSO.Customer.Controllers
{
    [RoutePrefix("cm")]
    public class CustomerController : ApiController
    {
        [Route("info/{id}")]
        [HttpGet]
        public Models.Customer Get(int id)
        {
            return new Models.Customer();
        }

        [Route("")]
        [HttpPost]
        public int Post(Models.Customer customer)
        {
            return 1;
        }
    }
}