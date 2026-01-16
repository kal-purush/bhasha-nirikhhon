using AutoMapper;
using DiaryApp.Data.DTO;
using DiaryApp.Core.Models;
using DiaryApp.Data.ServiceInterfaces.Lists;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Http;
using System.Threading.Tasks;

namespace DiaryApp.API.Controllers.Lists
{
    public class EventsController : CrudController<EventItemDto, EventItem>
    {
        private readonly IEventItemService _eventItemService;

        public EventsController(IEventItemService eventItemService, IMapper mapper, ILoggerFactory loggerFactory) : base(eventItemService, mapper, loggerFactory)
        {
            _eventItemService = eventItemService;
        }

        [HttpPost]
        [ProducesResponseType(StatusCodes.Status200OK)]
        public override async Task<IActionResult> PostAsync([FromBody] EventItemDto createModel)
        {
            createModel.Date = createModel.Date.ToLocalTime();
            return await base.PostAsync(createModel);
        }
    }
}