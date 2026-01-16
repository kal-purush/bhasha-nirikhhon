using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using vopen_api.Models;
using vopen_api.Repositories;

namespace vopen_api.Controllers
{
    [Route("api/v1/editions")]
    [ApiController]
    public class EditionsController : ControllerBase
    {
        private readonly EditionsRepository editionsRepository;

        public EditionsController(EditionsRepository editionsRepository)
        {
            this.editionsRepository = editionsRepository;
        }

        // GET api/v1/editions
        [HttpGet]
        public async Task<IEnumerable<EditionDTO>> GetAllAsync()
        {
            var language = this.GetLanguage();
            return await this.editionsRepository.GetAllByLanguage(language);
        }

        // GET api/v1/editions/5
        [HttpGet("{id}")]
        public async Task<IActionResult> GetByIdAsync(string id)
        {
            var language = this.GetLanguage();
            var result = await this.editionsRepository.GetByLanguageAndId(language, id);

            if (result == null)
            {
                return NotFound();
            }

            return Ok(result);
        }

        // POST api/v1/edition
        //[HttpPost]
        //public async Task<IActionResult> Post([FromBody] EditionDTO newEdition)
        //{
        //    var result = await this.editionsRepository.Create(newEdition);

        //    if (result == null)
        //    {
        //        return null;
        //    }

        //    return CreatedAtAction(nameof(GetByIdAsync), new { id = newEdition.Id }, newEdition);
        //}

        //// PUT api/v1/edition/5
        //[HttpPut("{id}")]
        //public async Task<IActionResult> Put(String id, [FromBody] EditionDTO updatedEdition)
        //{
        //    if (id != updatedEdition.Id)
        //    {
        //        return BadRequest();
        //    }

        //    var result = await this.editionsRepository.Update(updatedEdition);

        //    if (result == null)
        //    {
        //        return NotFound();
        //    }

        //    return Ok(result);
        //}

        //// DELETE api/v1/event/5
        //[HttpDelete("{id}")]
        //public async Task<IActionResult> Delete(string id)
        //{
        //    await this.editionsRepository.Delete(id);
        //    return NoContent();
        //}

        private string GetLanguage()
        {
            if (Request.Headers["Accept-Language"].ToString() == "")
            {
                return "es-AR";
            }

            return Request.Headers["Accept-Language"].ToString().Split(',')[0];
        }
    }
}