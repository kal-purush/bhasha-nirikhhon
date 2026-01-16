using System;
using System.Threading.Tasks;
using MediatR;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace FS.Api.Controllers
{
    
    [ApiController]
    [Route("v1")]
    public class AccountController  : ControllerBase
    {
        private readonly ILogger<UserController> _logger;
        private readonly IMediator _mediator;

        public AccountController(ILogger<UserController> logger, IMediator mediator)
        {
            _logger = logger;
            _mediator = mediator;
        }

        [HttpGet("account/{id}")]
        public async Task<ActionResult> GetAccount(Guid id)
        {
            return Ok();
        }
        
        [HttpGet("account/user/{id}")]
        public async Task<ActionResult> GetAccountByUserId(Guid id)
        {
            return Ok();
        }
    }
}