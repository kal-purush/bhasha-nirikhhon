using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using MVCAngular.Models;
using MongoDB.Driver;
using MongoDB.Bson;
using MongoDB.Driver;
namespace MVCAngular.Controllers.Api
{
    public class MGController : ApiController
    {
        // GET api/homes
        private UserContext _USContext;
        private MongoContext _MGContext;

        public IEnumerable<User> Get()
        {
           
            _MGContext = new MongoContext();
            var people = _MGContext.People.AsQueryable();
            var c = people.ToList<Person>();

            var result = new List<User>();
            c.ForEach(p => result.Add(new User() { LastName = p.LastName, FirstName = p.FirstName, Id = p.Id }));
            return result;
 
        }

        // GET api/homes/5
        public string Get(int id)
        {
            return "value";
        }

        // POST api/homes
        public void Post(User uData)
        {
              
            _MGContext = new MongoContext();
            uData.FirstName = !string.IsNullOrWhiteSpace(uData.FirstName) ? uData.FirstName.Trim() : string.Empty;
            uData.LastName = !string.IsNullOrWhiteSpace(uData.LastName) ? uData.LastName.Trim() : string.Empty;
            uData.Id = DateTime.Now.Millisecond;
            var person = new Person(uData);

            _MGContext.People.InsertOne(person);
        }

        // PUT api/homes/5
        public void Put(User uData)
        {
 
            _MGContext = new MongoContext();
            uData.FirstName = !string.IsNullOrWhiteSpace(uData.FirstName) ? uData.FirstName.Trim() : string.Empty;
            uData.LastName = !string.IsNullOrWhiteSpace(uData.LastName) ? uData.LastName.Trim() : string.Empty;
 
            var person = new Person(uData);
            var filter = Builders<Person>.Filter.Eq("_id", person.Id);
            var update = Builders<Person>.Update.Set("FirstName", person.FirstName).Set("LastName", person.LastName);
            var result = _MGContext.People.UpdateOneAsync(filter, update);
         }

        // DELETE api/homes/5
        [HttpDelete]
        public void Delete(int id)
        {
            _USContext = new UserContext();
            _USContext.Delete(id);

        }
    }
}