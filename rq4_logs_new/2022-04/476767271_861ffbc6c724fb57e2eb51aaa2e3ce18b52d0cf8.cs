using Bcp.Data;
using Bcp.Domain.Models;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace Bcp.Api.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class StockEntryController : ControllerBase
    {
        private readonly BcpContext _context;
        public StockEntryController(BcpContext context)
        { 
            _context = context;
        }
        // GET: api/<StockEntryController>
        [HttpGet]
        public IEnumerable<string> Get()
        {
            return new string[] { "value1", "value2" };
        }

        // GET api/<StockEntryController>/5
        [HttpGet("{id}")]
        public string Get(int id)
        {
            return "value";
        }

        // POST api/<StockEntryController>
        [HttpPost]
        public void Post([FromBody] string value)
        {
        }

        // PUT api/<StockEntryController>/5
        [HttpPut("{id}")]
        public void Put(int id, [FromBody] string value)
        {
        }

        // DELETE api/<StockEntryController>/5
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }

        [HttpGet("GetLabelValue")]
        public IActionResult GetLabelValue()
        {
            try
            {
                Lblnbgen lbl = new Lblnbgen();
                _context.lblnbgens.Add(lbl);
                _context.SaveChanges();              
                var res = lbl.LblId;
                _context.lblnbgens.Remove(lbl);
                _context.SaveChanges();
                return Ok(res);
            }
            catch (Exception ex)
            {
                return BadRequest(ex.Message);
            }

        }
        
    }
}