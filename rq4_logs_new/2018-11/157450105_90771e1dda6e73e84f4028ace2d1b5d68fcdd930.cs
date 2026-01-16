using Logatti.ByBus.API.Core;
using Logatti.ByBus.CrossCutting.Notifications;
using Logatti.ByBus.Domain.CommandHandler.Bus;
using MediatR;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

namespace Logatti.ByBus.API.Controllers
{
    public class BusController : ApiBaseController
    {
        private readonly IMediator mediator;
        private readonly ILogger<BusController> _logger;

        public BusController(INotificationHandler notificationHandler, ILogger<BusController> logger, IMediator mediator) : base(notificationHandler, logger)
        {
            this.mediator = mediator;
        }

        [Route("BusRouteById")]
        [HttpPost]
        public async Task<IActionResult> BusRouteById([FromBody] BusRouteByIdCommand command)
        {
            return await CreateResponse(async () => await mediator.Send(command));
        }
    }
}