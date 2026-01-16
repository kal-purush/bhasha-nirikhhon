using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using AutoMapper;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using QuestStore.API.Dtos.InDtos;
using QuestStore.Core.Interfaces;

namespace QuestStore.API.Controllers
{
    [Route("api/[ParentController]/{id1}/[ChildController]")]
    [ApiController]
    [ApiConventionType(typeof(DefaultApiConventions))]
    public class LinkingGenericController<T, TOut> : ControllerBase
    where T : class
    {
        protected readonly ILinkingRepository<T> Repository;
        protected readonly IMapper Mapper;
        private readonly string _errorMessage;

        public LinkingGenericController(ILinkingRepository<T> repository, IMapper mapper)
        {
            Repository = repository;
            Mapper = mapper;
            _errorMessage = "Database error";
        }

        [HttpGet]
        public virtual async Task<ActionResult<List<TOut>>> GetAllResources(int id1)
        {
            try
            {
                var result = await Repository.GetByFirstId(id1, 1);
                return Ok(Mapper.Map<List<TOut>>(result.ToList()));
            }
            catch (Exception)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, _errorMessage);
            }
        }

        [HttpGet("{id2}")]
        public virtual async Task<ActionResult<TOut>> GetResource(int id1, int id2)
        {
            try
            {
                var result = await Repository.GetByFullKey(id1, id2, 1);

                if (result == null) return NotFound();

                return Ok(Mapper.Map<TOut>(result));
            }
            catch (Exception)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, _errorMessage);
            }
        }

        [HttpPost("{id2}")]
        public virtual async Task<ActionResult<TOut>> CreateResource(int id1, int id2)
        {
            try
            {
                var resource = Mapper.Map<T>(new LinkingDto { Id1 = id1, Id2 = id2 });
                await Repository.Add(resource);
                resource = await Repository.GetByFullKey(id1, id2, 1);

                return CreatedAtAction(
                    nameof(GetResource),
                    new { id1 = id1, id2 = id2 },
                    Mapper.Map<TOut>(resource));
            }
            catch (Exception)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, _errorMessage);
            }
        }
    }
}