using Final.Models;
using Final.Services;
using Microsoft.AspNetCore.Mvc;

namespace Final.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class WorkoutPlanController : ControllerBase
    {
        private readonly IWorkoutPlanService _workoutPlanService;

        public WorkoutPlanController(IWorkoutPlanService workoutPlanService)
        {
            _workoutPlanService = workoutPlanService;
        }

        [HttpPost]
        public IActionResult CreateWorkoutPlan([FromBody] WorkoutPlan workoutPlan)
        {
            try
            {
                var createdWorkoutPlan = _workoutPlanService.CreateWorkoutPlan(
                    workoutPlan.UserId,
                    workoutPlan.WorkoutPlanName,
                    workoutPlan.WeekDay
                );

                return Ok(createdWorkoutPlan);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Outer exception: {ex.Message}");

                if (ex.InnerException != null)
                {
                    Console.WriteLine($"Inner exception: {ex.InnerException.Message}");
                }
                throw;
            }
        }

        [HttpGet("{userId}")]
        public IActionResult GetWorkoutPlansByUserId(string userId)
        {
            try
            {
                var workoutPlans = _workoutPlanService.GetWorkoutPlansByUserId(userId);
                return Ok(workoutPlans);
            }
            catch (Exception ex)
            {
                return StatusCode(500, "Internal Server Error: " + ex.Message);
            }
        }

        [HttpPut("{workoutPlanId}")]
        public IActionResult UpdateWorkoutPlan(int workoutPlanId, [FromBody] WorkoutPlan workoutPlan)
        {
            try
            {
                var updatedWorkoutPlan = _workoutPlanService.UpdateWorkoutPlan(
                    workoutPlanId,
                    workoutPlan.WorkoutPlanName,
                    workoutPlan.WeekDay
                );

                if (updatedWorkoutPlan != null)
                    return Ok(updatedWorkoutPlan);
                else
                    return NotFound();
            }
            catch (Exception ex)
            {
                return BadRequest(ex.Message);
            }
        }

        [HttpDelete("{workoutPlanId}")]
        public IActionResult DeleteWorkoutPlan(int workoutPlanId)
        {
            try
            {
                var result = _workoutPlanService.DeleteWorkoutPlan(workoutPlanId);
                if (result)
                    return Ok();
                else
                    return NotFound();
            }
            catch (Exception ex)
            {
                return BadRequest(ex.Message);
            }
        }
    }
}