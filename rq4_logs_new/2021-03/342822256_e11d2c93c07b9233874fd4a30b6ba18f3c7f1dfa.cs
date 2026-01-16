using HostelWebAPI.DataAccess.Interfaces;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace HostelWebAPI.Controllers
{
    [Route("api/reservation")]
    [ApiController]
    public class ReservationHistoryController : ControllerBase
    {
        private readonly IDbRepo repo;

        public ReservationHistoryController(IDbRepo repo)
        {   
            this.repo = repo;
        }

        [HttpGet]
        public async Task<IActionResult> GetReservationByPropertyId(
            [FromQuery(Name = "propertyId")] string propertyId,
            [FromQuery(Name = "userId")] string userId)
        {
            var reserv = await repo.ReservationHistories.GetByPropertyIdAsync(propertyId);
            return Ok(reserv);
        }


    }
}