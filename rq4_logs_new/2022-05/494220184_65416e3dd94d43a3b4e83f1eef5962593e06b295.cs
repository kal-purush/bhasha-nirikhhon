using DL;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.ModelBinding;
using Models;

namespace Pokemon_Vanquish_Proj.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class Admin : ControllerBase
    {
        private IUserRepoNew repo;
        private static List<User> users = new List<User>();
        public Admin(IUserRepoNew repo)
        {
            this.repo = repo;
        }
        /// <summary>
        /// Gets all the users from the database and returns them.
        /// </summary>
        /// <returns>A list of all Users in the Database</returns>
        [HttpGet]
        [Route("Get All Users")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        public ActionResult<List<User>> Get()
        {
            try
            {
                users = repo.GetAllUsers();
            }
            catch (Exception ex)
            {
                Log.Information("Bad Request exception in get user @ Admin.");
                return BadRequest(ex.Message);
            }
            Log.Information("Good request at Get User @ Admin.");
            return Ok(users);
        }
        /// <summary>
        /// Takes in a user ID and deletes them from the DB
        /// </summary>
        /// <param name="id"></param>
        /// <returns>Returns 200 if the user was deleted. 400 if not.</returns>
        [HttpDelete]
        [Route("Remove User")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        public ActionResult Delete([FromQuery][BindRequired] int id)
        {
            try
            {
                repo.DeleteUser(id);
                users = repo.GetAllUsers();
            }
            catch (Exception ex)
            {
                Log.Information("Bad Request exception in delete user @ Admin.");
                return BadRequest(ex.Message);
            }
            Log.Information("Good request at delete User @ Admin.");
            return Ok();
        }

    }
}