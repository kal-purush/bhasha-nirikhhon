using Event_Planning_System.IServices;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace Event_Planning_System.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class TestStorageController : ControllerBase
    {
        readonly IBlobServices BlobServices;
        public TestStorageController(IBlobServices _blob)
        {
            BlobServices = _blob;
        }
        [HttpPost]
        public async Task<IActionResult> uploadimg([FromForm] IFormFile image)
        {
            string s  = await BlobServices.AddingImage(image);
            return Ok(s);

        }
    }
}