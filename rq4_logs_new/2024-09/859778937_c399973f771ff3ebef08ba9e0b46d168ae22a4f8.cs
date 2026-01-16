using Hackathon.AI.Services;
using Microsoft.AspNetCore.Mvc;

namespace AITest.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class StorageController : ControllerBase
    {
        private readonly BlobStorageService _blobService;

        public StorageController(BlobStorageService blobService)
        {
            _blobService = blobService;
        }

        [HttpPost("Video")]
        public async Task<IActionResult> UploadVideo([FromForm] FormFileCollection video)
        {
            Stream stream = video.First().OpenReadStream();
            return Ok(await _blobService.UploadFileAsync(stream));
        }
    }
}