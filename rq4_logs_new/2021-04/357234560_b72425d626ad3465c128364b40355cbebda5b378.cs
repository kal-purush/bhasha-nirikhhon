using Frontend.Models;
using Frontend.Services;
using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;

namespace Frontend.Controllers.api
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

            return CreatedAtRoute("GetHistory", new { id = history.Id.ToString() }, history);
        }

        [HttpPut("{id:length(24)}")]
        public IActionResult Update(string id, History historyIn)
        {
            var history = _historyService.Get(id);

            if (history == null)
            {
                return NotFound();
            }

            _historyService.Update(id, historyIn);

            return NoContent();
        }

        [HttpDelete("{id:length(24)}")]
        public IActionResult Delete(string id)
        {
            var history = _historyService.Get(id);

            if (history == null)
            {
                return NotFound();
            }

            _historyService.Remove(history.Id);

            return NoContent();
        }
    }

}