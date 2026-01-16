using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using LeadMgmt.Api.Model;
using LeadMgmt.Api.Repository;
using Microsoft.AspNetCore.Mvc;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace LeadMgmt.Api.Controllers
{
    [Route("api/[controller]")]
    public class LeadsController : Controller
    {
        private readonly ILeadsRepository _repository;

        public LeadsController(ILeadsRepository repository)
        {
            _repository = repository;
        }
        // GET: api/<controller>
        [HttpGet]
        public async Task<IActionResult> GetAll()
        {
            var model = await _repository.GetAll();
            return Ok(model);
        }

        // GET api/<controller>/5
        [HttpGet("{id}")]
        public async Task<IActionResult> GetById(long id)
        {
            if (id <= 0)
            {
                return BadRequest();
            }

            var item = await _repository.GetById(id);
            if (item != null)
            {
                return Ok(item);
            }

            return NotFound();
        }

        // POST api/<controller>
        [HttpPut]
        public async Task<IActionResult> Put([FromBody] Lead lead)
        {
            try
            {
                if (!ModelState.IsValid) return BadRequest(ModelState);
                var oldLead = _repository.GetById(lead.ID);
                if (oldLead == null)
                    return NotFound($"Couldn't find a lead of {lead.ID}");
                await _repository.Update(lead);
                return Ok();
            }
            catch (Exception)
            {
                return BadRequest("Could not update lead");
            } 
        }


        [HttpPost]
        public async Task<IActionResult> Post([FromBody]Lead lead)
        {
            try
            {
                await _repository.Create(lead);
                return Ok("Save Successfull");
            }
            catch (Exception ex)
            {
                return BadRequest("Could not save");
            }
        }

        // DELETE api/<controller>/5
        [HttpDelete("{id}")]
        public async Task<IActionResult> Delete(long id)
        {
            try
            {
                var oldLead = _repository.GetById(id);
                if (oldLead == null) return NotFound($"Couldn’t found lead of id {id}");
                await _repository.Delete(id);
                return Content("Deleted Successfully");
            }
            catch (Exception)
            {
                return BadRequest("Couldn’t Delete lead");
            }
        }
    }
}