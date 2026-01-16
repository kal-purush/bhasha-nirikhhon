using Microsoft.AspNetCore.Mvc;
using MyBlazorApp.Server.Interfaces;
using MyBlazorApp.Shared.Models;

namespace MyBlazorApp.Server.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class VacationControler : ControllerBase
    {
        private readonly IVacationService _vacationService;

        public VacationControler(IVacationService vacationServices)
        {
            _vacationService = vacationServices;
        }

        [HttpGet]
        public ActionResult<List<VacationDto>> GetAll()
        {
            return Ok(_vacationService.GetVacations());
        }


        [HttpGet("{Id}")]

        public ActionResult<ExistingVacationDto> Get(int id)
        {
            var Id = _vacationService.GetVacation(id);
            if (Id != null)
            {
                return Ok(Id);
            }
            else
            {
                return NotFound();
            }
        }

        [HttpPost]
        public ActionResult Post([FromBody] NewVacationDto vacation)
        {
            try
            {
                _vacationService.AddVacation(vacation);
                return Ok(ModelState);
            }
            catch (Exception ex)
            {
                ModelState.AddModelError("vacation", ex.Message);
                return BadRequest(ModelState);
            }
            
        }

        [HttpPut]
        public void Put(ExistingVacationDto vacation)
        {
            _vacationService.UpdateVacation(vacation);
        }

        [HttpDelete("{Id}")]
        public IActionResult Delete(int Id)
        {
            _vacationService.DeleteVacation(Id);
            return Ok();
        }
    }
}