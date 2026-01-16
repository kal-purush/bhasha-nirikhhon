using DemoAPI.Model;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using System.Web.Http.Description;

namespace DemoAPI.Controllers
{
    [RoutePrefix("api/v1")]
    public class PersonController : ApiController
    {
        private IPersonRepository personRepository = new PersonRepository();

        [Route("hello")]
        [HttpGet]
        public IHttpActionResult HelloWorld()
        {
            return Content(System.Net.HttpStatusCode.OK, "Hello!");
        }

        [Route("add")]
        [HttpPost]
        [ResponseType(typeof(Person))]
        public IHttpActionResult Post([FromBody] Person person)
        {
            personRepository.Add(person);

            return Ok();
        }

        [Route("getperson/{id}")]
        [HttpGet]
        public IHttpActionResult Get(int id)
        {
            Person person = personRepository.Get(id);
            return Content(HttpStatusCode.OK, JsonConvert.SerializeObject(person));
        }
    }
}