using MediatR;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CQRS_MediatR.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class HomeController : ControllerBase
    {
        private Dictionary<int, string> list = new Dictionary<int, string>()
        { { 1,"value1"} ,{ 2,"value2"},{ 3,"value3"},{ 4,"value4"},{ 5,"value5"}};

        private readonly IMediator _mediator;
        public HomeController(IMediator mediator) => _mediator = mediator;

        [HttpGet()]
        public IEnumerable<string> Get()
        {
            return list.Values;
        }

        [HttpGet("{id}")]
        public string Get(int id)
        {
            return list.Where(x => x.Key == id).FirstOrDefault().Value;
        }

        // POST api/<ValuesController>
        [HttpPost]
        public void Add(string value)
        {
            var k = list.Keys.ToList().OrderBy(x => x).Last() + 1;
            list.Add(k, value);
        }

        // PUT api/<ValuesController>/5
        [HttpPut("{id}")]
        public IEnumerable<string> Edit(int id, string value)
        {
            var item = list.FirstOrDefault(x => x.Key == id);
            if (item.Value != null)
            {
                list.Remove(id);
                list.Add(id, value);
            }
            return list.Values;
        }


        // DELETE api/<ValuesController>/5
        [HttpDelete("{id}")]
        public IEnumerable<string> Delete(int id)
        {
            var item = list.FirstOrDefault(x => x.Key == id);
            if (item.Value != null)
            {
                list.Remove(id);
            }
            return list.Values;
        }
    }
}