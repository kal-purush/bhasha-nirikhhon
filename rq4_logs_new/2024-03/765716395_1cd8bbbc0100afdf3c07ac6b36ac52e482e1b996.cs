using EtudeManyToMany.API.Repository;
using EtudeManyToMany.Core.Model;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace EtudeManyToMany.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ReservationController : ControllerBase
    {
        private readonly IRepository<Reservation> _reservationsRepository;

        public ReservationController(IRepository<Reservation> reservationsRepository)
        {
            _reservationsRepository = reservationsRepository;
        }

        [HttpGet]
        public async Task<IActionResult> TouteLesReservations()
        {
            var reservations = await _reservationsRepository.GetAll();

            return Ok(reservations);
        }


        [HttpGet("{reservationId}")]
        public async Task<IActionResult> Get(int reservationId)
        {
            var reservations = await _reservationsRepository.GetById(reservationId);

            return Ok(reservations);
        }
    }
}