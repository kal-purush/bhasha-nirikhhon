using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using AgendaApi.Entities;
using AgendaApi.Data.Repository.Interfaces;
using AgendaApi.DTOs;

namespace AgendaApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class UserController : ControllerBase
    {

        private IUserRepository _userRepository { get; set; }
        public UserController(IUserRepository userRepository)
        {
            _userRepository = userRepository;
        }

        [HttpPost]
        
        public IActionResult CreateUser(UserForCreationDTO dto)
        {
            try
            {
                _userRepository.CreateUser(dto);
            }

            catch(Exception ex)
            {

                return BadRequest( ex.Message);
            }

            return Created("Created", dto);
        }

     
    }
}