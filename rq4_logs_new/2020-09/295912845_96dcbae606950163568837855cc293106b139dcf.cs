using AutoMapper;
using Microsoft.AspNetCore.Mvc;
using OffiRent.API.Domain.Models;
using OffiRent.API.Domain.Services;
using OffiRent.API.Resources;
using Supermarket.API.Extensions;
using Swashbuckle.AspNetCore.Annotations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace OffiRent.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class PublicationController : ControllerBase
    {
        private readonly IPublicationService _publicationServices;
        private readonly IMapper _mapper;

        public PublicationController(IPublicationService publicationServices, IMapper mapper)
        {
            _publicationServices = publicationServices;
            _mapper = mapper;
        }

        [SwaggerOperation(
           Summary = "List all Publications"
           )]
        [SwaggerResponse(200, "List of Publications", typeof(IEnumerable<PublicationResource>))]
        [ProducesResponseType(typeof(IEnumerable<PublicationResource>), 200)]
        [HttpGet]
        public async Task<IEnumerable<PublicationResource>> GetAllAsync()
        {
            var publications = await _publicationServices.ListAsync();
            var resources = _mapper.Map<IEnumerable<Publication>, IEnumerable<PublicationResource>>(publications);
            return resources;
        }

        [SwaggerOperation(
            Summary = "Create a Publication"
        )]
        [SwaggerResponse(200, "Publication was created", typeof(PublicationResource))]
        [HttpPost]
        public async Task<IActionResult> PostAsync([FromBody] SavePublicationResource resource)
        {
            if (!ModelState.IsValid)
                return BadRequest(ModelState.GetErrorMessages());

            var publication = _mapper.Map<SavePublicationResource, Publication>(resource);

            var result = await _publicationServices.SaveAsync(publication);

            if (!result.Success)
                return BadRequest(result.Message);

            var publicationResource = _mapper.Map<Publication, PublicationResource>(result.Resource);

            return Ok(publicationResource);

        }

        [SwaggerOperation(
            Summary = "Update a Publication"
        )]
        [SwaggerResponse(200, "Publication was updated", typeof(PublicationResource))]
        [HttpPut("id")]
        public async Task<IActionResult> PutAsync(int id, [FromBody] SavePublicationResource resource)
        {
            var publication = _mapper.Map<SavePublicationResource, Publication>(resource);
            var result = await _publicationServices.UpdateAsync(id, publication);

            if (!result.Success)
                return BadRequest(result.Message);
            var publicationResource = _mapper.Map<Publication, PublicationResource>(result.Resource);
            return Ok(publicationResource);
        }
    }
}