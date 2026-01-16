using System;
using System.Linq;
using System.Threading.Tasks;
using System.Web.Http;
using HQF.Tutorial.WebAPI.Indentity2.Infrastructure;

namespace HQF.Tutorial.WebAPI.Indentity2.Controllers
{
    [RoutePrefix("api/accounts")]
    public class AccountsController : BaseApiController
    {
        [Route("users")]
        public IHttpActionResult GetUsers()
        {
            return Ok(AppUserManager.Users.ToList().Select(u => TheModelFactory.Create(u)));
        }

        [Route("user/{id:guid}", Name = "GetUserById")]
        public async Task<IHttpActionResult> GetUser(string Id)
        {
            var user = await AppUserManager.FindByIdAsync(Id);

            if (user != null)
            {
                return Ok(TheModelFactory.Create(user));
            }

            return NotFound();
        }

        [Route("user/{username}")]
        public async Task<IHttpActionResult> GetUserByName(string username)
        {
            var user = await AppUserManager.FindByNameAsync(username);

            if (user != null)
            {
                return Ok(TheModelFactory.Create(user));
            }

            return NotFound();
        }

        /// <summary>
        /// </summary>
        /// <remarks>
        ///     Post Body eg.
        ///     {
        ///     "Email":"works4you@163.com",
        ///     "UserName":"test",
        ///     "Password":"test@123",
        ///     "ConfirmPassword":"test@123",
        ///     "FirstName":"QF",
        ///     "LastName":"Huo",
        ///     }
        /// </remarks>
        /// <param name="createUserModel"></param>
        /// <returns></returns>
        [Route("create")]
        public async Task<IHttpActionResult> CreateUser(CreateUserBindingModel createUserModel)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            var user = new ApplicationUser
            {
                UserName = createUserModel.Username,
                Email = createUserModel.Email,
                FirstName = createUserModel.FirstName,
                LastName = createUserModel.LastName,
                Level = 3,
                JoinDate = DateTime.Now.Date
            };

            var addUserResult = await AppUserManager.CreateAsync(user, createUserModel.Password);

            if (!addUserResult.Succeeded)
            {
                return GetErrorResult(addUserResult);
            }

            var locationHeader = new Uri(Url.Link("GetUserById", new {id = user.Id}));

            return Created(locationHeader, TheModelFactory.Create(user));
        }
    }
}