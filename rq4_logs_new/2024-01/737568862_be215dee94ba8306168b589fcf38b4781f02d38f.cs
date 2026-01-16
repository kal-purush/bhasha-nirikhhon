using HCI_Task.Server.Data;
using HCI_Task.Server.Data.DTOs;
using HCI_Task.Server.Entities;
using HCI_Task.Server.Repositories.HospotalRepository;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace HCI_Task.Server.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class HospitalController(IHospitalRepository hospitalRepo) : ControllerBase
    {
        [HttpPost]
        public async Task<ActionResult<List<Hospital>>> SearchUserHospitalsNames([FromBody] UserHospitalSearchDTO search)
        {

            var hospitals = await hospitalRepo.SearchUserHospitalByName(search);

            return Ok(hospitals);
        }
    }
}