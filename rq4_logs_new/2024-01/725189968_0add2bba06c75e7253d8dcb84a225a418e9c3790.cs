using System.Net;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using RestaurauntApp.DTOS;
using RestaurauntApp.Repositories.Base;

namespace RestaurauntApp.Controllers
{
     [Route("[controller]/[action]")]
    public class AccountController : Controller
    {
         private readonly IAccountRepository accountRepository;

        public AccountController(IAccountRepository repository)
        {
          this.accountRepository = repository;
        }

        [HttpGet]
        [AllowAnonymous]
        public IActionResult Register()
        {
            return View();
        }
        
         [HttpPost]
        [AllowAnonymous]
        public async Task<IActionResult> Register(UserDTO newUser)
        {
            try
            {
                var rowsAffected = await accountRepository.CreateAccountsync(newUser);

                if (rowsAffected > 0)
                {
                    // Insertion was successful
                  return Ok(rowsAffected);
                }
                else
                {
                    // Insertion failed
                    return StatusCode((int)HttpStatusCode.InternalServerError, "Failed to insert user.");
                }
            }
            catch (Exception)
            {
                return StatusCode((int)HttpStatusCode.InternalServerError, "An error occurred while processing the request.");
            }
        }
    }

}