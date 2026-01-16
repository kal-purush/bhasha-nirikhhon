using AutoMapper;
using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;
using System.Threading.Tasks;
using Talkish.Domain.Interfaces;
using Talkish.Domain.Models;

namespace Talkish.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class PublicationsController : ControllerBase
    {
        private readonly IPublicationService _service;
        private readonly IMapper _mapper;
    
        public PublicationsController(IPublicationService service, IMapper mapper)
        {
            _service = service;
            _mapper = mapper;
        }

        [HttpPost]
        public async Task<IActionResult> CreatePublication([FromBody] Publication PublicationData)
        {
            Publication publication = await _service.CreatePublication(PublicationData);
            return Ok(publication);
        }

        [HttpGet]
        public async Task<IActionResult> GetAllPublications()
        {
            List<Publication> publications = await _service.GetAllPublications();
            return Ok(publications);
        }

        [HttpGet]
        [Route("{Id}")]
        public async Task<IActionResult> GetPublicationById([FromRoute] int Id)
        {
            Publication publication = await _service.GetPublicationById(Id);
            return Ok(publication);
        }

        [HttpGet]
        [Route("{Id}/Authors")]
        public async Task<IActionResult> GetPublicationAuthors([FromRoute] int Id)
        {
            List<Author> authors = await _service.GetPublicationAuthors(Id);
            return Ok(authors);
        }

        [HttpGet]
        [Route("{Id}/Blogs")]
        public async Task<IActionResult> GetPublicationBlogs([FromRoute] int Id)
        {
            List<Blog> blogs = await _service.GetPublicationBlogs(Id);
            return Ok(blogs);
        }

        [HttpPatch]
        public async Task<IActionResult> UpdatePublication([FromBody] Publication PublicationData)
        {
            Publication publication = await _service.UpdatePublication(PublicationData);
            return Ok(publication);
        }

        [HttpDelete]
        [Route("{Id}")]
        public async Task<IActionResult> DeletePublication([FromRoute] int Id)
        {
            Publication publication = await _service.DeletePublication(Id);
            return Ok(publication);
        }
    }
}