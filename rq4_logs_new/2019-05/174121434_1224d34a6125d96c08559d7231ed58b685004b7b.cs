using System;
using System.Collections.Generic;
using System.Security.Claims;
using DfE.EmployerFavourites.ApplicationServices.Commands;
using DfE.EmployerFavourites.Web.Security;
using MediatR;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace DfE.EmployerFavourites.Api.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ApprenticeshipsController : ControllerBase
    {
        private readonly ILogger<ApprenticeshipsController> _logger;
        private readonly IMediator _mediator;

        public ApprenticeshipsController(ILogger<ApprenticeshipsController> logger, IMediator mediator)
        {
            _logger = logger;
            _mediator = mediator;
        }

        // GET api/values
        [HttpGet]
        public ActionResult<IEnumerable<string>> Get()
        {
            try
            {
                var userId = User.FindFirstValue(EmployerClaims.IdamsUserIdClaimTypeIdentifier);

                var apprenticeships = _mediator.Send(new GetApprenticeshipFavouritesRequest() {UserId = userId});

                return Ok(apprenticeships);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return BadRequest();
            }
        }
    }
}