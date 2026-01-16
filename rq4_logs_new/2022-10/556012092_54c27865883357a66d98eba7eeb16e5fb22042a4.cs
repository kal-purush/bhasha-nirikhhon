using AgendaApi.Data.Repository.Interfaces;
using AgendaApi.Entities;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace AgendaApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ContactController : ControllerBase
    {
        private readonly IContactRepository _contactRepository;

        public ContactController(IContactRepository contactRepository)
        {
            _contactRepository = contactRepository;
        }
        [HttpGet]
        [Route("GetContacts/{userId}")]
        public IActionResult GetAllContactsByUserId(int userId)
        {
            try
            {
                return Ok(_contactRepository.GetAllContactsByUserId(userId));
            }
            catch
            {
                return BadRequest();
            }
        }
        [HttpGet]
        [Route("{id}")]
        public IActionResult GetById(int id)
        {
            try
            {
                return Ok(_contactRepository.GetById(id));
            }
            catch
            {
                return NotFound("Contacto no existente");
            }
            
        }
     
    }
    
}