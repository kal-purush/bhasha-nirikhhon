using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using SpaceParkBackend.Database;
using SpaceParkBackend.Models;

namespace SpaceParkBackend.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class ParkingGuardController : ControllerBase
    {
        private readonly ILogger<ParkingGuardController> _logger;
        private readonly IParkingGuardRepository ParkingGuard;
        public ParkingGuardController(ILogger<ParkingGuardController> logger, IParkingGuardRepository parkingGuard)
        {
            _logger = logger;
            ParkingGuard = parkingGuard;
        }

        [HttpGet]
        public ActionResult<Parkinglot> Get(SpaceparkContext context)
        {
            var response = ParkingGuard.CheckOpenStatus(context);

            return Ok(response);
        }
    }
}