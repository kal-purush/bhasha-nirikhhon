using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace MvcWebApp.API
{
    public class MatchApiController : ApiController
    {
        // GET: api/MatchApi
        public IEnumerable<string> Get()
        {
            return new string[] { "value1", "value2" };
        }

        // GET: api/MatchApi/5
        public string Get(int id)
        {
            return "value";
        }

        // POST: api/MatchApi
        public void Post([FromBody]string value)
        {
            int a;
        }

        // PUT: api/MatchApi/5
        public void Put(int id, [FromBody]string value)
        {
        }

        // DELETE: api/MatchApi/5
        public void Delete(int id)
        {
        }
    }
}