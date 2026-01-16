using Microsoft.AspNetCore.Mvc;
using toiec_web.Services;
using toiec_web.Services.IService;

namespace toiec_web.Controllers
{
    public class TestPartController : BaseAPIController
    {
        private readonly ITestPartService _testPartService;

        public TestPartController(ITestPartService testPartService)
        {
            _testPartService = testPartService;
        }
        [HttpGet]
        [Route("GetAllTestParts")]
        public async Task<IActionResult> GetAllTestParts()
        {
            var listTopic = await _testPartService.GetAllTestParts();
            if (listTopic == null)
            {
                return StatusCode(StatusCodes.Status404NotFound);
            }
            return Ok(listTopic);
        }
    }
}