using Microsoft.AspNetCore.Mvc;
using toiec_web.Services;
using toiec_web.Services.IService;

namespace toiec_web.Controllers
{
    public class RecordController : BaseAPIController
    {
        private readonly IRecordService _recordService;

        public RecordController(IRecordService recordService) 
        {
            _recordService = recordService;
        }

        [HttpGet]
        [Route("GetRecordByUserTest/{userId}&&{testId:guid}")]
        public async Task<IActionResult> GetRecordByUserTest(string userId, Guid testId)
        {
            var unit = await _recordService.GetRecordByUserTest(userId, testId);
            if (unit == null)
            {
                return StatusCode(StatusCodes.Status404NotFound);
            }
            return Ok(unit);
        }
    }
}