using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using QuestStore.Core.Entities;
using QuestStore.Core.Interfaces;

namespace QuestStore.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    [ApiConventionType(typeof(DefaultApiConventions))]
    public class GenericController<T> : ControllerBase where T : BaseEntity, new()
    {
        protected readonly IRepository<T> Repository;
        private readonly string _errorMessage = "Database error";

        public GenericController(IRepository<T> repository)
        {
            Repository = repository;
        }

        [HttpGet]
        public virtual async Task<ActionResult<List<T>>> GetAllResources()
        {
            try
            {
                var result = await Repository.GetAll();
                return Ok(result.ToList());
            }
            catch (DbException)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, _errorMessage);
            }
        }

        [HttpGet("{id}")]
        public virtual async Task<ActionResult<T>> GetResource(int id)
        {
            try
            {
                var result = await Repository.GetById(id);

                if (result == null) return NotFound();

                return Ok(result);
            }
            catch (DbException)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, _errorMessage);
            }
        }

        [HttpPost]
        public virtual async Task<ActionResult<T>> CreateResource(T resource)
        {
            try
            {
                await Repository.Add(resource);
                return CreatedAtAction(nameof(GetResource), new {id = resource.Id}, resource);
            }
            catch (DbUpdateException)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, _errorMessage);
            }
        }

        [HttpPut("{id}")]
        public virtual async Task<ActionResult<T>> UpdateResource(int id, T resource)
        {
            try
            {
                if (id != resource.Id) return BadRequest();

                await Repository.Update(resource);
            }
            catch (DbUpdateConcurrencyException)
            {
                if (await Repository.GetById(id) == null) return NotFound();
            }
            catch (DbException)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, _errorMessage);
            }

            return NoContent(); //The operation was successful
        }

        [HttpDelete("{id}")]
        public virtual async Task<IActionResult> DeleteResource(int id)
        {
            try
            {
                var resource = await Repository.GetById(id);

                if (resource == null) return NotFound();

                await Repository.DeleteById(id);
                return Ok();
            }
            catch (DbException)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, _errorMessage);
            }
        }
    }
}