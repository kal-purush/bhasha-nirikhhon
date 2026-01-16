using Microsoft.AspNetCore.Mvc;
using Igtampe.Neco.Common;
using Igtampe.Neco.Data;
using Igtampe.ChopoSessionManager;
using Microsoft.EntityFrameworkCore;
using Igtampe.Neco.API.Requests;

namespace Igtampe.Neco.API.Controllers {

    /// <summary>Controller that handles User operations</summary>
    [Route("API/Users")]
    [ApiController]
    public class UserController : ControllerBase {

        private readonly NecoContext DB;

        /// <summary>Creates a User Controller</summary>
        /// <param name="Context"></param>
        public UserController(NecoContext Context) => DB = Context;

        #region Gets
        /// <summary>Gets a directory of this Neco server</summary>
        /// <param name="Query">Search query to search in IDs and </param>
        /// <param name="Take"></param>
        /// <param name="Skip"></param>
        /// <returns></returns>
        [HttpGet("Dir")]
        public async Task<IActionResult> Directory([FromQuery] string? Query, [FromQuery] int? Take, [FromQuery] int? Skip) {
            IQueryable<User> Set = DB.User;
            if (!string.IsNullOrWhiteSpace(Query)) { Set = Set.Where(U => U.ID != null && U.ID.Contains(Query) || U.Name.ToLower().Contains(Query.ToLower())); }
            Set = Set.Skip(Skip ?? 0).Take(Take ?? 20);

            return Ok(await Set.ToListAsync());
        }

        /// <summary>Gets username of the currently logged in session</summary>
        /// <param name="SessionID">ID of the session</param>
        /// <returns></returns>
        [HttpGet]
        public async Task<IActionResult> GetCurrentLoggedIn([FromHeader] Guid? SessionID) {
            Session? S = await Task.Run(() => SessionManager.Manager.FindSession(SessionID ?? Guid.Empty));
            if (S is null) { return NotFound(ErrorResult.NotFound("Session was not found")); }

            //Get the user
            return await GetUser(S.UserID);
        }

        /// <summary>Gets a given user</summary>
        /// <param name="ID"></param>
        /// <returns></returns>
        [HttpGet("{ID}")]
        public async Task<IActionResult> GetUser(string ID) {
            //Get the user
            User? U = await DB.User.Include(U => U.Roles).FirstOrDefaultAsync(U => U.ID == ID);
            return U is null ? NotFound(ErrorResult.NotFound("User was not found")) : Ok(U);
        }

        /// <summary>Gets a logged in user's notifications</summary>
        /// <param name="SessionID">Session ID of the user you wish to retrieve notificaitons from</param>
        /// <returns></returns>
        [HttpGet("Notifs")]
        public async Task<IActionResult> GetNotifs([FromHeader] Guid? SessionID) {
            Session? S = await Task.Run(() => SessionManager.Manager.FindSession(SessionID ?? Guid.Empty));
            return S is null
                ? Unauthorized(ErrorResult.Reusable.InvalidSession)
                : Ok(await DB.Notification.Where(N => N.User != null && N.User.ID == S.UserID).OrderByDescending(N => N.Date).ToListAsync());
        }

        #endregion

        #region Puts

        /// <summary>Handles user password changes</summary>
        /// <param name="Request">Request with their current and new password</param>
        /// <param name="SessionID">ID of the session executing this request</param>
        /// <returns></returns>
        // PUT api/Users
        [HttpPut]
        public async Task<IActionResult> Update([FromHeader] Guid? SessionID, [FromBody] ChangePasswordRequest Request) {

            //Ensure nothing is null
            if (Request.New is null) { return BadRequest("Cannot have empty password"); }

            //Check the session:
            Session? S = await Task.Run(() => SessionManager.Manager.FindSession(SessionID ?? Guid.Empty));
            if (S is null) { return Unauthorized("Invalid session"); }

            //Check the password
            User? U = await DB.User.FirstOrDefaultAsync(U => U.ID == S.UserID && U.Password == Request.Current);
            if (U is null) { return BadRequest("Incorrect current password"); }

            U.Password = Request.New;
            DB.Update(U);
            await DB.SaveChangesAsync();

            return Ok();

        }

        #endregion

        #region Posts

        // POST api/Users
        /// <summary>Handles logging in to Clothespin</summary>
        /// <param name="Request">Request with a User and Password attempt to log in</param>
        /// <returns></returns>
        [HttpPost]
        public async Task<IActionResult> LogIn(LoginRequest Request) {
            if (Request.ID is null || Request.Password is null) { return BadRequest("ID or Password is null"); }

            //Check the user on the DB instead of the user de-esta cosa
            bool Login = await DB.User.AnyAsync(U => U.ID == Request.ID && U.Password == Request.Password);
            if (!Login) { return Ok("ID or Password is incorrect"); }

            //Generate a session
            return Ok(SessionManager.Manager.LogIn(Request.ID));

        }

        /// <summary>Handles user registration</summary>
        /// <param name="Request">User and password combination to create</param>
        /// <returns></returns>
        // POST api/Users/register
        [HttpPost("register")]
        public async Task<IActionResult> Register(RegistrationRequest Request) {
            if (string.IsNullOrWhiteSpace(Request.Name)) { return BadRequest(ErrorResult.BadRequest("Name cannot be empty", "Name")); }
            if (string.IsNullOrWhiteSpace(Request.Password)) { return BadRequest(ErrorResult.BadRequest("Password cannot be empty", "Password")); }

            string ID = "";

            User NewUser = new() {
                Name = Request.Name,
                Password = Request.Password,
                Roles = new()
            };

            if (!await DB.User.AnyAsync()) {

                //This is the first account and *MUST* be an admin
                NewUser.Roles.Admin = true;

            }

            //With any luck this will only need to run once
            do { ID = NewUser.IDGenerator.Generate(); } while (await DB.User.AnyAsync(u => u.ID == ID));

            //Hey we didn't set this no wonder this happened
            NewUser.ID = ID;

            DB.User.Add(NewUser);
            await DB.SaveChangesAsync();

            return Ok(NewUser);

        }

        /// <summary>Handles user logout</summary>
        /// <param name="SessionID">Session to log out of</param>
        /// <returns></returns>
        // POST api/Users/out
        [HttpPost("out")]
        public async Task<IActionResult> LogOut([FromBody] Guid SessionID) => Ok(await Task.Run(() => SessionManager.Manager.LogOut(SessionID)));

        /// <summary>Handles user logout of *all* sessions</summary>
        /// <param name="SessionID">Session that wants to log out of all tied sessions</param>
        /// <returns></returns>
        // POST api/Users/outall
        [HttpPost("outall")]
        public async Task<IActionResult> LogOutAll([FromBody] Guid SessionID) {
            //Check the session:
            Session? S = await Task.Run(() => SessionManager.Manager.FindSession(SessionID));
            return S is null ? Unauthorized("Invalid session") : Ok(await Task.Run(() => SessionManager.Manager.LogOutAll(S.UserID)));
        }

        /// <summary>Request to reset the password of a user</summary>
        /// <param name="SessionID">SessionID of an administrator who wishes to change the password of another user</param>
        /// <param name="Request">Request to change</param>
        /// <returns></returns>
        [HttpPost("Reset")]
        public async Task<IActionResult> ResetPassword([FromHeader] Guid? SessionID, [FromBody] ResetPasswordRequest Request) {
            //Ensure nothing is null
            if (Request.User is null || Request.New is null) { return BadRequest("Cannot have empty password"); }

            //Check the session:
            Session? S = await Task.Run(() => SessionManager.Manager.FindSession(SessionID ?? Guid.Empty));
            if (S is null) { return Unauthorized("Invalid session"); }

            //Get Users
            User? Executor = await DB.User.Include(U=>U.Roles).FirstOrDefaultAsync(U => U.ID == S.UserID);
            if (Executor is null) { return Unauthorized("Invalid Session"); }
            if (Executor.Roles is null || !Executor.Roles.Admin) { return Forbid(); }

            User? U = await DB.User.FirstOrDefaultAsync(U => U.ID == Request.User);
            if (U is null) { return NotFound("User was not found"); }

            U.Password = Request.New;
            DB.Update(U);
            await DB.SaveChangesAsync();

            return Ok();

        }

        #endregion

        #region Deletes

        /// <summary>Deletes a notification</summary>
        /// <param name="SessionID"></param>
        /// <param name="ID"></param>
        /// <returns></returns>
        [HttpDelete("Notifs/{ID}")]
        public async Task<IActionResult> DeleteNotif([FromHeader] Guid? SessionID, Guid ID) {
            Session? S = await Task.Run(() => SessionManager.Manager.FindSession(SessionID ?? Guid.Empty));
            if (S is null) { return Unauthorized(ErrorResult.Reusable.InvalidSession); }

            //Get the notification
            Notification? N = await DB.Notification.FirstOrDefaultAsync(N=>N.ID==ID && N.User != null && N.User.ID==S.UserID);
            if (N is null) { return NotFound(ErrorResult.NotFound("Notification was not found")); }

            DB.Remove(N);
            await DB.SaveChangesAsync();

            return Ok(N);

        }

        #endregion
    }
}