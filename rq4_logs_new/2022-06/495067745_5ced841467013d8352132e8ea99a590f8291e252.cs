using MyBlazorApp.Shared.Models;
using Microsoft.AspNetCore.Mvc;
using MyBlazorApp.Server.Interfaces;

namespace MyBlazorApp.Server.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class UserVacationBudgetController : ControllerBase
    {
        private readonly IUserVacationBudgetService _userVacationBudget;

        public UserVacationBudgetController (IUserVacationBudgetService userVacationBudgetService)
        {
            _userVacationBudget= userVacationBudgetService;
        }

        [HttpGet]
        public ActionResult<List<UserVacationBudgetDto>> GetAll()
        {
            return Ok(_userVacationBudget.GetUserVacationsBudget());
        }

        [HttpGet("{id}")]
        public ActionResult<UserVacationBudgetDto> Get(int id)
        {
            var user = _userVacationBudget.GetUserVacationBudgetDto(id);
            if (user != null)
            {
                return Ok(user);
            }
            else
            {
                return NotFound();
            }
        }

        [HttpPost]
        public ActionResult Post([FromBody] UserVacationBudgetDto user)
        {
            try
            {
                _userVacationBudget.AddUserVacationBudget(user);
                return Ok(ModelState);
            }
            catch (Exception ex)
            {
                ModelState.AddModelError("year", ex.Message);
                return BadRequest(ModelState);
            }
        }

        [HttpPut]
        public void Put(UserVacationBudgetDto user)
        {
            _userVacationBudget.UpdateUserVacationBudget(user);
        }

        [HttpDelete("{id}")]
        public IActionResult Delete(int id)
        {
            _userVacationBudget.DeleteUserVacationBudgetDto(id);
            return Ok();
        }

    }
}