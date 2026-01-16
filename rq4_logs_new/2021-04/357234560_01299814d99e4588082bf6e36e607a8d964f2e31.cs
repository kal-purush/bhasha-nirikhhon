using Backend.Models;
using Backend.Services;
using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;

namespace Backend.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class HistorysController : ControllerBase
    {
        private readonly HistoryService _historyService;

        public HistorysController(HistoryService historyService)
        {
            _historyService = historyService;
        }

        [HttpGet]
        public ActionResult<List<History>> Get() =>
            _historyService.Get();

        [HttpGet("{id:length(24)}", Name = "GetHistory")]
        public ActionResult<History> Get(string id)
        {
            var history = _historyService.Get(id);

            if (history == null)
            {
                return NotFound();
            }

            return history;
        }

        [HttpPost]
        public ActionResult<History> Create(History history)
        {
            _historyService.Create(history);

            return CreatedAtRoute("Gethistory", new { id = history.Id.ToString() }, history);
        }
    }

}