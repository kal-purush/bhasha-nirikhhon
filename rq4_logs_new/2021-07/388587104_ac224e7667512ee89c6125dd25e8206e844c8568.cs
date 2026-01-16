using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MonsterHotel.Repositories;

namespace MonsterHotel.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class TicketController : ControllerBase
    {
        private readonly ITicketRepository _ticketRepository;
        //private readonly IUserProfileRepository _userProfileRepository;
        public TicketController(ITicketRepository ticketRepository)
        {
            _ticketRepository = ticketRepository;
        }
        //public TicketController(IUserProfileRepository userProfileRepository)
        //{
        //    _userProfileRepository = userProfileRepository;
        //}
        [HttpGet]
        public IActionResult Get()
        {
            return Ok(_ticketRepository.GetAll());
        }
    }
}