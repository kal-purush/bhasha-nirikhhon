using Microsoft.AspNetCore.Mvc;
using System;
using Zignificant.Models.Requests;
using Zignificant.Models.Responses;
using Zignificant.Repository;

namespace Zignificant.Api.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class HistoryController : ControllerBase
    {
        private IHistoryRepository _historyRepository;

        [HttpPut("{historyId:int}")]
        public IActionResult UpdateHistory([FromRoute] int historyId, [FromBody] HistoryUpdateRequest req)
        {
            try
            {
                _historyRepository.Update(req);
                return Ok();
            }
            catch (Exception e)
            {
                return StatusCode(500, e);
            }
        }

        [HttpPost]
        public IActionResult CreateHistory([FromBody] HistoryCreateRequest req)
        {
            HistoryResponse resp = _historyRepository.Create(req);
            return CreatedAtAction("GetHistoryById", new { historyId = resp.Id }, resp);
        }

        [HttpDelete("{historyId:int}")]
        public IActionResult DeleteHistoryById([FromRoute] int historyId)
        {
            try
            {
                _historyRepository.Delete(historyId);
                return Ok();
            }
            catch (Exception e)
            {
                return StatusCode(500, e);
            }
        }

        [HttpGet("{historyId:int}", Name = "GetHistoryById")]
        public IActionResult GetHistoryById([FromRoute] int historyId)
        {
            try
            {
                var history = _historyRepository.GetRecordById(historyId);
                return Ok(history);
            }
            catch (Exception e)
            {
                return StatusCode(500, e);
            }
        }

        [HttpGet]
        public IActionResult GetAllHistory()
        {
            try
            {
                var history = _historyRepository.GetAll();
                return Ok(history);
            }
            catch (Exception e)
            {
                return StatusCode(500, e);
            }
        }

        public HistoryController(IHistoryRepository historyRepository)
        {
            _historyRepository = historyRepository;
        }
    }

}