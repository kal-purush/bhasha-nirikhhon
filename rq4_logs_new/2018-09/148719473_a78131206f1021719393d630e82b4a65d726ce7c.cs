using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using com.methodcorp.security;
using System.Web.Http.Results;  

namespace com.methodcorp.security.api.Controllers
{
    [RoutePrefix("security/Domain")]
    public class DomainController : ApiController
    {
        SecurityStorage Storage = new SecurityStorage();

        [HttpGet]
        [Route("{domainName}", Name = "GetDomain")]
        public IHttpActionResult GetDomain(string domainName)
        {
            DomainModel domain = Storage.getDomain(domainName);
            if (domain != null)
            {
                return Ok(domain);
            }
            else
            {
                return NotFound();
            }
        }//GetDomain

        [HttpDelete]
        [Route("{domainName}")]
        public IHttpActionResult DeleteDomain(string domainName)
        {
            long output = Storage.deleteDomain(domainName);
            StatusCodeResult result = new StatusCodeResult(HttpStatusCode.NoContent, this);
            return result;
        }//DeleteDomain

        [HttpPut]
        [Route("{domainName}")]
        public IHttpActionResult UpdateDomain(long domainId,string domainName)
        {
            if(domainId == 0 || domainName.Equals(null) || domainName == string.Empty)
            {
                return BadRequest("enter a value for domain name");
            }
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }
            long output = Storage.updateDomain(domainId, domainName);
            return new StatusCodeResult(HttpStatusCode.NoContent, this);

        }//UpdateDomain

        [HttpPost]
        [Route("")]
        public IHttpActionResult CreateDomain([FromBody] string domainName)
        {
            if (domainName.Equals(null) || domainName == string.Empty)
            {
                return BadRequest("enter a value for domain name");
            }
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }
            long domainId = Storage.createDomain(domainName);
            return CreatedAtRoute("GetDomain", new { domainName = domainName }, domainName);
        }//CreateDomain
    }//DomainController
}//com.methodcorp.security.api.Controllers